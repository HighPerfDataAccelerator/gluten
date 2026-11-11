#!/usr/bin/env python3
"""link.py <velox-build-dir> <out.so> [extra link flags...]: relink libvelox.so from control link command, swapping Velox build dir."""
import json,subprocess,sys
from pathlib import Path
old='/home/nvidia/ferd/.deps/velox-before-decoder/_build/release'
new,out=sys.argv[1],sys.argv[2];extra=sys.argv[3:]
src=Path('/home/nvidia/ferd/benchmarks/vera-stateless-validation-20261105/native-build/candidate')
c=json.load(open(src/'link-command.json'))
c=[x.replace(old,new) for x in c]
c=[new+"/lib/libvelox.a" if x.endswith("build-join128/libvelox.a") else x for x in c]
c=[x for x in c if not x.startswith('-Wl,--dependency-file=') and not x.startswith('-Wl,-Map')]
i=c.index('-o');c[i+1]=out
# drop reference to old control libvelox.so path tokens if any (output only)
c[1:1]=extra
Path(out).parent.mkdir(parents=True,exist_ok=True)
(Path(out).parent/(Path(out).name+'.link-command.json')).write_text(json.dumps(c,indent=1))
subprocess.run(c,cwd='/home/nvidia/ferd/code/gluten/cpp/build',check=True)
