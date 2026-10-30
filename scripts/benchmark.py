"""Run a reproducible benchmark and record process peak RSS when psutil is installed."""
import argparse, json, os, subprocess, sys, time, threading
from pathlib import Path
try:
    import psutil
except ImportError:
    psutil=None

def main():
    p=argparse.ArgumentParser(); p.add_argument("--records",type=int,default=100000); p.add_argument("--chunk-size",type=int,default=5000); p.add_argument("--work-dir",default="data/benchmark"); a=p.parse_args()
    root=Path(__file__).resolve().parents[1]; work=Path(a.work_dir); work.mkdir(parents=True,exist_ok=True)
    # Benchmarks must start from a clean target so reruns do not only measure deduplication.
    import shutil
    for stale in (work/"bench.sqlite3", work/"output"):
        if stale.is_dir(): shutil.rmtree(stale)
        elif stale.exists(): stale.unlink()
    subprocess.run([sys.executable,str(root/"scripts"/"generate_data.py"),"--out-dir",str(work),"--records",str(a.records)],check=True,cwd=root)
    cmd=[sys.executable,"-m","streametl.cli","--input",str(work/"orders.csv"),"--customers",str(work/"customers.csv"),"--db",str(work/"bench.sqlite3"),"--output-dir",str(work/"output"),"--chunk-size",str(a.chunk_size)]
    peak=[0]; stop=threading.Event(); start=time.perf_counter()
    env=os.environ.copy(); env["PYTHONPATH"]=str(root/"src")
    if psutil:
        child=subprocess.Popen(cmd,cwd=root,env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True)
        try: child_proc=psutil.Process(child.pid)
        except psutil.Error: child_proc=None
        def monitor():
            while not stop.is_set():
                try:
                    if child_proc: peak[0]=max(peak[0],child_proc.memory_info().rss)
                except psutil.Error: pass
                time.sleep(.01)
        t=threading.Thread(target=monitor,daemon=True); t.start()
        stdout,stderr=child.communicate(); code=child.returncode
        elapsed=round(time.perf_counter()-start,4); stop.set(); t.join()
    else:
        completed=subprocess.run(cmd,cwd=root,env=env,capture_output=True,text=True)
        stdout,stderr,code=completed.stdout,completed.stderr,completed.returncode
        elapsed=round(time.perf_counter()-start,4)
    report={"records_requested":a.records,"chunk_size":a.chunk_size,"elapsed_seconds":elapsed,"pipeline_process_peak_rss_mb":round(peak[0]/1024/1024,2) if psutil else None,"measurement_note":"RSS is sampled for the pipeline process when psutil is installed; short-lived peaks can be missed by polling. Record OS/runtime details when reporting results.","return_code":code,"stdout_tail":stdout[-2000:],"stderr_tail":stderr[-2000:]}
    (work/"benchmark_report.json").write_text(json.dumps(report,indent=2),encoding="utf-8"); print(json.dumps(report,indent=2)); sys.exit(code)
if __name__=="__main__": main()
