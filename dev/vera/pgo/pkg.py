#!/usr/bin/env python3
"""pkg.py <variant dir name> <libvelox.so> <variant label>: copy control jar swapping linux/aarch64/libvelox.so"""
import zipfile,sys,json,hashlib,pathlib
D=pathlib.Path('/home/nvidia/ferd/benchmarks/vera-pgo-20261106')
name,lib,label=sys.argv[1:4]
src=D/'control/gluten.jar';out=D/name;out.mkdir(exist_ok=True)
sha=lambda b:hashlib.sha256(b).hexdigest()
libb=pathlib.Path(lib).read_bytes()
with zipfile.ZipFile(src) as zi,zipfile.ZipFile(out/'gluten.jar','w',zipfile.ZIP_DEFLATED,compresslevel=6) as zo:
    for i in zi.infolist():
        data=zi.read(i.filename)
        if i.filename=='linux/aarch64/libvelox.so':data=libb
        zo.writestr(i,data,compress_type=i.compress_type)
pk=json.loads((D/'control/package.json').read_text())
pk['control_jar_sha256']=pk['jar_sha256']
pk['jar_sha256']=sha((out/'gluten.jar').read_bytes())
pk['native_libraries']['linux/aarch64/libvelox.so']=sha(libb)
pk['variant']=label;pk['changed_jar_entries']=['linux/aarch64/libvelox.so'];pk.pop('added_classes',None)
(out/'package.json').write_text(json.dumps(pk,indent=2))
print(json.dumps(pk,indent=1))
