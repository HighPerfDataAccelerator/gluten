import json,statistics as st,sys
D="/home/nvidia/ferd/benchmarks/vera-pgo-20261106/"
s=json.load(open(D+"full-status.json"))
res={}
for r in s["runs"]:
    qs=r["queries"];tot=sum(q["seconds"] for q in qs);busy=sum(q["seconds"]*q["host_busy_percent"] for q in qs)/tot
    res.setdefault((r["suite"],r["arm"]),[]).append(dict(name=r["name"],tot=tot,busy=busy,q={q["query"]:q["seconds"] for q in qs}))
    g=json.load(open(D+"results/"+r["name"]+"/launch.json"));c=json.load(open(D+r["label"]+"-cgroup-status.json"))
    print(r["name"],r["arm"],round(tot,2),"busy",round(busy,1),"correct",r["correct"],"eligible",g["performance_eligible"],"swapIO MiB",round(c["host_swap_io_bytes"]/2**20,2))
for suite in ["tpcds","tpch"]:
    c=res[(suite,"control")];p=res[(suite,"pgo")]
    mc=st.mean(x["tot"] for x in c);mp=st.mean(x["tot"] for x in p)
    print(suite,"control",[round(x["tot"],2) for x in c],"pgo",[round(x["tot"],2) for x in p],"mean",round(mc,2),round(mp,2),"delta %",round(100*(mp-mc)/mc,2),"busy",round(st.mean(x["busy"] for x in c),1),round(st.mean(x["busy"] for x in p),1))
    reg=[]
    for q in c[0]["q"]:
        a=st.mean(x["q"][q] for x in c);b=st.mean(x["q"][q] for x in p);reg.append((b-a,q,a,b))
    reg.sort()
    print(" worst:",[(q,round(a,2),round(b,2)) for d,q,a,b in reg[-6:]])
    print(" best:",[(q,round(a,2),round(b,2)) for d,q,a,b in reg[:6]])
    short=[(q,a,b) for d,q,a,b in reg if a<1.0]
    print(" short<1s n",len(short),"ctl",round(sum(a for _,a,_ in short),2),"pgo",round(sum(b for *_,b in short),2),"n>10%slower",sum(1 for _,a,b in short if b>1.1*a),"n>25%slower",sum(1 for _,a,b in short if b>1.25*a))
    print(" pairs ctl1/pgo1",round(c[0]["tot"],1),round(p[0]["tot"],1),"pgo2/ctl2",round(p[1]["tot"],1),round(c[1]["tot"],1))
