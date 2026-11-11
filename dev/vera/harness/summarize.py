import pathlib,json,statistics,collections,sys
r=pathlib.Path(__file__).resolve().parent;tag=sys.argv[1] if len(sys.argv)>1 else 'screen';s=json.loads((r/(tag+'-status.json')).read_text());rows=[]
for run in s['runs']:
 d=r/'results'/run['name'];qs=run['queries'];total=sum(q['seconds'] for q in qs)
 row={'run':run['name'],'suite':run['suite'],'conf':run['conf'],'seconds':total,'correct':run['correct'],'busy':sum(q['seconds']*q['host_busy_percent'] for q in qs)/total,'iowait':sum(q['seconds']*q['host_iowait_percent'] for q in qs)/total,'executor_cpu_seconds':sum(q['seconds']*q['executor_cores'] for q in qs),'queries':[]}
 for q in qs:
  qr={k:q[k] for k in ['query','seconds','host_busy_percent','host_iowait_percent']};scans=[];seen=set()
  def walk(n):
   if 'id' not in n or n['id'] in seen:return
   seen.add(n['id'])
   if n['class']=='FileSourceScanExecTransformer':scans.append({k:v['value'] for k,v in n['metrics'].items()})
   for c in n['children']:walk(c)
  f=d/(q['query']+'-metrics.json')
  if f.exists():walk(json.loads(f.read_text()))
  if scans:
   m=max(scans,key=lambda m:m.get('storageReadBytes',0));qr['largest_scan']={k:m.get(k) for k in ['storageReadBytes','storageReads','ioWaitTime','pageLoadTime','rawInputRows','processedSplits','processedStrides','preloadSplits','peakMemoryBytes']}
  row['queries'].append(qr)
 resource=d/'resources.jsonl'
 if resource.exists():
  samples=[json.loads(x) for x in resource.read_text().splitlines()];row['min_host_available_GiB']=min(x['memory']['MemAvailable'] for x in samples)/2**30;row['peak_executor_rss_GiB']=max(sum(e['rss'] for e in x['executors']) for x in samples)/2**30;row['max_executor_swap_GiB']=max(x['executor_swap_bytes'] for x in samples)/2**30
 rows.append(row)
groups=[]
for suite,conf in sorted(set((x['suite'],x['conf']) for x in rows)):
 xs=[x for x in rows if x['suite']==suite and x['conf']==conf];seconds=sum(x['seconds'] for x in xs)
 groups.append({'suite':suite,'conf':conf,'repeats':len(xs),'seconds':[x['seconds'] for x in xs],'mean_seconds':statistics.mean(x['seconds'] for x in xs),'busy':sum(x['seconds']*x['busy'] for x in xs)/seconds,'iowait':sum(x['seconds']*x['iowait'] for x in xs)/seconds,'executor_cpu_seconds':statistics.mean(x['executor_cpu_seconds'] for x in xs),'queries':{q['query']:[next(z['seconds'] for z in x['queries'] if z['query']==q['query']) for x in xs] for q in xs[0]['queries']}})
out={'status':s['status'],'groups':groups,'runs':rows,'verified_queries':sum(len(x['queries']) for x in rows)};(r/(tag+'-summary.json')).write_text(json.dumps(out,indent=2))
for x in groups:print(x['suite'],x['conf'],'seconds',[round(t,3) for t in x['seconds']],'mean',round(x['mean_seconds'],3),'busy',round(x['busy'],2),'iowait',round(x['iowait'],2))
