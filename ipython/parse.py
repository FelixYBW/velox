appid="app-20250815070400-0006"
!mkdir -p /mnt/data1/tablescan_test/$appid/8
!ssh ip "docker cp dev:/opt/spark/work/$appid/8/stdout ./"; scp ip:~/stdout /mnt/data1/tablescan_test/$appid/8/

jsonfile='48io-8rg-16split-x'
with open(f"/mnt/data1/tablescan_test/{appid}/8/stdout") as f:
    out=f.readlines()
out2=[l.replace(": "," ").replace("]","] ").replace("\t"," ").replace("\n","") for l in out]
out3=[]
#out4=[]
starttime=0
dur=0
st=0
last_end=0
last_start=-1
for l in out2:
    t=re.search("LATENCY_BREAKDOWN \[(.+)\] (\d+) (\d+) (\d+) (\d+)",l)
    if t:
        if starttime==0:
            starttime = int(t.group(3))
        if int(t.group(4))>dur:
            dur=int(t.group(4))
        if int(t.group(3))-starttime>500670677 or int(t.group(4))>20000000:
            continue
        slipstart=int(t.group(3))-starttime+1
        dur=int(t.group(4))
        
        out3.append({
            'tid':int(t.group(2)[-5:]),
            'ts':slipstart,
            'dur':dur,
            'pid':0,
            'ph':'X',
            'name':t.group(1),
            "args":{"size":human_format(int(t.group(5)))}
        })
        last_end=slipstart+dur
        last_start=slipstart
        #out4.append([int(t.group(2)[-5:]),slipstart,dur,t.group(1),int(t.group(5))])
        
dfx=pandas.DataFrame.from_dict(out3)
dfx2=dfx.sort_values(by=["ts",'dur'],ascending=[True,False])
js=dfx2.to_dict(orient='records')
jsd=json.dumps(js)
with open(f'/mnt/data1/traceview/{jsonfile}.json','w') as w:
    w.write('''
        {
            "traceEvents":
        ''')
    w.write(jsd)
    w.write('''
        }''')

print(f'http://bwdaily1.fyre.ibm.com:1088/tracing_examples/trace_viewer.html#/tracing/test_data/{jsonfile}.json')
