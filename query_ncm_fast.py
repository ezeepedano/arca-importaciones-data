#!/usr/bin/env python3
from __future__ import annotations
import argparse, hashlib, html, json, re, time, zipfile
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

UA="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/153 Safari/537.36"
URLS=[
 "https://www.arca.gob.ar/operadoresComercioExterior/informacionAgregada/download.aspx?filename={periodo}.zip",
 "https://www.afip.gob.ar/operadoresComercioExterior/informacionAgregada/download.aspx?filename={periodo}.zip",
]

def norm(s):
    return re.sub(r"\s+"," ",html.unescape(s or "").strip())
def nh(s):
    s=norm(s).upper()
    for a,b in [("Á","A"),("É","E"),("Í","I"),("Ó","O"),("Ú","U"),("Ñ","N")]:
        s=s.replace(a,b)
    return re.sub(r"[^A-Z0-9]+","_",s).strip("_")
def sf(s):
    try:return float(str(s).strip().replace(",","."))
    except:return None
def val(parts,i):
    return norm(parts[i]) if i is not None and i<len(parts) else ""
def idx(headers, aliases, fallback=None):
    for i,h in enumerate(headers):
        if h in aliases:return i
    return fallback
def contains(headers, needles, fallback=None):
    for i,h in enumerate(headers):
        if any(n in h for n in needles):return i
    return fallback
def tokg(q,u):
    if q is None:return None
    if u in {"01","21","53"}:return q
    if u=="29":return q*1000
    if u in {"14","22","23"}:return q/1000
    if u=="41":return q/1_000_000
    return None
def download(periodo,tmp):
    tmp.mkdir(parents=True,exist_ok=True)
    dest=tmp/f"{periodo}.zip"
    for base in URLS:
        url=base.format(periodo=periodo)
        for attempt in range(4):
            try:
                req=Request(url,headers={"User-Agent":UA,"Accept":"application/zip,*/*"})
                with urlopen(req,timeout=180) as r, dest.open("wb") as f:
                    while True:
                        chunk=r.read(8*1024*1024)
                        if not chunk:break
                        f.write(chunk)
                if zipfile.is_zipfile(dest):
                    return dest,url
            except (HTTPError,URLError,OSError,TimeoutError):
                time.sleep(2*(attempt+1))
        dest.unlink(missing_ok=True)
    raise RuntimeError(f"No se pudo descargar {periodo}")

def scan(periodo,prefix,out):
    zpath,source=download(periodo,out.parent/".tmp")
    matches=[]; seen=set()
    with zipfile.ZipFile(zpath) as z:
        names=[n for n in z.namelist() if Path(n).name.lower().startswith("impo_") and n.lower().endswith(".lst")]
        if not names: raise RuntimeError("No impo lst")
        member=names[0]
        with z.open(member) as f:
            header=f.readline().decode("latin-1",errors="replace").rstrip("\r\n")
            H=[nh(x) for x in header.split("'")]
            # 2022+ detailed schema. Resolve by headers first, then stable fallbacks.
            desti=contains(H,{"DESTINACION"},1)
            item=contains(H,{"NUM_ITEM","ITEM"},2)
            fecha=contains(H,{"FECHA"},3)
            imp=contains(H,{"NOMBRE_IMPORTADOR","IMPORTADOR"},4)
            uni=idx(H,{"UN","UNIDAD","UNIDAD_MEDIDA"},6)
            cant=contains(H,{"CANT_UNIDAD_MEDIDA","CANTIDAD"},7)
            fob=idx(H,{"FOB_DOLAR","VALOR_ITEM"},8)
            if fob is None:fob=contains(H,{"FOB_DOLAR","VALOR_ITEM"},8)
            pais=[i for i,h in enumerate(H) if h in {"PAI","PAIS","PAIS_ORIGEN","PAIS_PROCEDENCIA"} or h.startswith("PAI")]
            po=pais[0] if len(pais)>=1 else 11
            pp=pais[1] if len(pais)>=2 else 12
            ncm=contains(H,{"POS_NCM","NCM"},14 if len(H)>=17 else 13)
            declared=None
            for i,h in enumerate(H):
                if "NCM" not in h and ("POS" in h or "POSICION" in h) and ("ARAN" in h or "DECL" in h):
                    declared=i;break
            if declared is None and len(H)>=17:declared=13
            reqmax=max(desti,item,fecha,imp,uni,cant,fob,po,pp,ncm,declared or 0)
            # Reject aggregate-era layout: requested window should all be detailed.
            if "CANT_DECLARACIONES" in H:
                raise RuntimeError(f"{periodo} unexpectedly aggregate")
            for raw in f:
                parts=raw.decode("latin-1",errors="replace").rstrip("\r\n").split("'")
                if len(parts)<=reqmax:continue
                nd=re.sub(r"\D","",val(parts,ncm))
                if not nd.startswith(prefix):continue
                key=(val(parts,desti),val(parts,item),nd)
                if key in seen:continue
                seen.add(key)
                q=sf(val(parts,cant)); u=val(parts,uni)
                matches.append({
                    "periodo":periodo,"fecha":val(parts,fecha),"importador":val(parts,imp),
                    "destinacion":key[0],"item":key[1],"ncm":val(parts,ncm),"ncm_digits":nd,
                    "posicion_arancelaria_declarada":val(parts,declared),
                    "pais_origen":val(parts,po),"pais_procedencia":val(parts,pp),
                    "unidad":u,"cantidad":q,"cantidad_kg":tokg(q,u),
                    "valor_item_usd":sf(val(parts,fob))
                })
    result={"periodo":periodo,"ncm_prefix":prefix,"source_url":source,"zip_member":member,
            "match_count":len(matches),"matches":matches}
    out.parent.mkdir(parents=True,exist_ok=True)
    out.write_text(json.dumps(result,ensure_ascii=False),encoding="utf-8")
    return result

if __name__=="__main__":
    ap=argparse.ArgumentParser()
    ap.add_argument("--periodo",required=True);ap.add_argument("--ncm-prefix",default="290613")
    ap.add_argument("--out",required=True)
    a=ap.parse_args();r=scan(a.periodo,a.ncm_prefix,Path(a.out))
    print(json.dumps({"periodo":a.periodo,"matches":r["match_count"]}))
