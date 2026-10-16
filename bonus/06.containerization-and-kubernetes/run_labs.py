"""Exécute les exercices sur le seul profil local ece-tp06 et garde les preuves.

Usage : python -X utf8 run_labs.py 1|2|3|all
Prérequis : exécuter start.ps1, puis lancer ce script depuis Windows.
"""
from pathlib import Path
import base64
import json
import os
import re
import subprocess
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parent
RUNTIME = ROOT / '.runtime'
RESULTS = ROOT / 'results'
RESULTS.mkdir(exist_ok=True)
ENV = os.environ.copy()
ENV['MINIKUBE_HOME'] = str(RUNTIME / 'minikube-home')
ENV['KUBECONFIG'] = str(RUNTIME / 'kubeconfig')
MK = [str(RUNTIME / 'minikube.exe'), '-p', 'ece-tp06']
LOG = None


def record(message):
    LOG.write(message + '\n')
    LOG.flush()


def run(command, *, timeout=360, check=True, input=None):
    record('$ ' + subprocess.list2cmdline(command))
    result = subprocess.run(command, env=ENV, cwd=ROOT, input=input,
                            text=True, encoding='utf-8', errors='replace',
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            timeout=timeout)
    record(result.stdout.rstrip())
    if result.stderr:
        record(result.stderr.rstrip())
    if check and result.returncode:
        raise RuntimeError(f'Commande échouée ({result.returncode}): {command}\n{result.stderr}')
    return result.stdout


def k(*args, **kwargs):
    return run(MK + ['kubectl', '--', *map(str, args)], **kwargs)


def obj(*args):
    return json.loads(k(*args, '-o', 'json'))


def apply(path):
    return k('apply', '-f', ROOT / path)


def wait_pods(namespace, count, label='app=kubernetes-bootcamp'):
    deadline = time.monotonic() + 180
    while time.monotonic() < deadline:
        pods = obj('-n', namespace, 'get', 'pods', '-l', label)['items']
        active = [p for p in pods if not p['metadata'].get('deletionTimestamp')]
        ready = [p for p in active if any(c['type'] == 'Ready' and c['status'] == 'True'
                                        for c in p.get('status', {}).get('conditions', []))]
        if len(pods) == count and len(ready) == count:
            return [p['metadata']['name'] for p in ready]
        time.sleep(2)
    raise TimeoutError(f'{namespace}: attendu {count} pods Ready, constaté {len(ready)}')


class ServiceTunnel:
    def __init__(self, service):
        self.path = RUNTIME / 'service-tunnel.log'
        self.file = self.path.open('w', encoding='utf-8')
        self.proc = subprocess.Popen(MK + ['service', service, '--url'], env=ENV,
                                     stdout=self.file, stderr=subprocess.STDOUT,
                                     creationflags=subprocess.CREATE_NO_WINDOW)
        self.url = None
        try:
            deadline = time.monotonic() + 90
            while time.monotonic() < deadline:
                text = self.path.read_text(encoding='utf-8', errors='replace')
                match = re.search(r'http://127\.0\.0\.1:\d+', text)
                if match:
                    self.url = match.group()
                    self.get()
                    record('Tunnel minikube service: ' + self.url)
                    return
                if self.proc.poll() is not None:
                    raise RuntimeError(text)
                time.sleep(1)
            raise TimeoutError('Tunnel minikube indisponible')
        except BaseException:
            self.close()
            raise

    def get(self):
        request = Request(self.url, headers={'Connection': 'close'})
        with urlopen(request, timeout=5) as response:
            assert response.status == 200
            return response.read().decode().strip()

    def close(self):
        # Arrête aussi le processus SSH créé par minikube, uniquement ce tunnel.
        subprocess.run(['taskkill', '/PID', str(self.proc.pid), '/T', '/F'],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.proc.wait(timeout=15)
        self.file.close()


def lab1():
    run(MK + ['status'])
    k('get', 'nodes', '-o', 'wide')
    k('describe', 'node', 'ece-tp06')
    k('-n', 'kube-system', 'get', 'pods', '-o', 'wide')
    # Laisse le serveur de métriques collecter son premier échantillon.
    k('-n', 'kube-system', 'rollout', 'status', 'deployment/metrics-server', '--timeout=180s')
    for attempt in range(30):
        try:
            k('top', 'pods', '-A', '--sort-by=cpu', '--sum=true')
            break
        except RuntimeError:
            if attempt == 29:
                raise
            time.sleep(3)
    k('create', 'deployment', 'kubernetes-bootcamp',
      '--image=gcr.io/google-samples/kubernetes-bootcamp:v1')
    k('rollout', 'status', 'deployment/kubernetes-bootcamp', '--timeout=240s')
    pod = wait_pods('default', 1)[0]
    k('logs', pod)
    k('describe', 'pod', pod)
    k('exec', pod, '--', 'cat', '/etc/os-release')
    k('exec', pod, '--', 'bash', '-c', 'pwd; ls -la /; cat /server.js; curl -s localhost:8080')
    k('expose', 'deployment/kubernetes-bootcamp', '--type=NodePort', '--port=8080')
    k('get', 'services', '-o', 'wide')
    run(MK + ['ip'])
    tunnel = ServiceTunnel('kubernetes-bootcamp')
    try:
        first = tunnel.get()
        record('Réponse v1: ' + first)
        assert 'v=1' in first
        k('scale', 'deployment/kubernetes-bootcamp', '--replicas=5')
        five = wait_pods('default', 5)
        responses = [tunnel.get() for _ in range(30)]
        record('30 requêtes après scale à 5:\n' + '\n'.join(responses))
        hit_pods = sorted({p for p in five if any(p in r for r in responses)})
        assert len(hit_pods) > 1, 'Répartition du trafic non observée'
        k('scale', 'deployment/kubernetes-bootcamp', '--replicas=2')
        two = wait_pods('default', 2)
        samples = []
        stop = threading.Event()

        def observe():
            while not stop.is_set():
                now = datetime.now(timezone.utc).isoformat()
                try:
                    samples.append({'at': now, 'response': tunnel.get(), 'ok': True})
                except Exception as exc:
                    samples.append({'at': now, 'error': str(exc), 'ok': False})
                stop.wait(0.5)

        observer = threading.Thread(target=observe, daemon=True)
        observer.start()
        try:
            time.sleep(1)
            k('set', 'image', 'deployment/kubernetes-bootcamp',
              'kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v2')
            k('rollout', 'status', 'deployment/kubernetes-bootcamp', '--timeout=240s')
            time.sleep(2)
            assert 'v=2' in tunnel.get()
            k('set', 'image', 'deployment/kubernetes-bootcamp',
              'kubernetes-bootcamp=jocatalin/kubernetes-bootcamp:v3')
            deadline = time.monotonic() + 180
            errors = []
            while time.monotonic() < deadline:
                failed = obj('get', 'pods', '-l', 'app=kubernetes-bootcamp')['items']
                errors = [s.get('state', {}).get('waiting', {}).get('reason')
                          for p in failed for s in p.get('status', {}).get('containerStatuses', [])]
                if any(e in ('ImagePullBackOff', 'ErrImagePull') for e in errors):
                    break
                time.sleep(2)
            assert any(e in ('ImagePullBackOff', 'ErrImagePull') for e in errors)
            k('get', 'pods', '-o', 'wide')
            k('describe', 'deployment', 'kubernetes-bootcamp')
            assert 'v=2' in tunnel.get()
            k('rollout', 'undo', 'deployment/kubernetes-bootcamp')
            k('rollout', 'status', 'deployment/kubernetes-bootcamp', '--timeout=120s')
            assert 'v=2' in tunnel.get()
            k('rollout', 'history', 'deployment/kubernetes-bootcamp')
            k('rollout', 'undo', 'deployment/kubernetes-bootcamp', '--to-revision=1')
            k('rollout', 'status', 'deployment/kubernetes-bootcamp', '--timeout=120s')
            assert 'v=1' in tunnel.get()
            time.sleep(1)
        finally:
            stop.set()
            observer.join(timeout=10)
            (RESULTS / 'lab-1-http.json').write_text(json.dumps(samples, indent=2), encoding='utf-8')
    finally:
        tunnel.close()
    k('delete', 'service', 'kubernetes-bootcamp')
    k('delete', 'deployment', 'kubernetes-bootcamp', '--wait=true')
    # Première application déclarative à 1 réplique, puis fichier final à 3.
    manifest = (ROOT / 'lab-1/deployment.yaml').read_text().replace('replicas: 3', 'replicas: 1')
    k('apply', '-f', '-', input=manifest)
    k('rollout', 'status', 'deployment/kubernetes-bootcamp', '--timeout=120s')
    wait_pods('default', 1)
    apply('lab-1/service.yaml')
    apply('lab-1/deployment.yaml')
    three = wait_pods('default', 3)
    tunnel = ServiceTunnel('kubernetes-bootcamp-service')
    try:
        declarative = [tunnel.get() for _ in range(30)]
        record('30 requêtes sur les 3 répliques déclaratives:\n' + '\n'.join(declarative))
        hit_three = sorted({p for p in three if any(p in r for r in declarative)})
        assert len(hit_three) > 1
    finally:
        tunnel.close()
    k('delete', '-f', 'lab-1/service.yaml', '-f', 'lab-1/deployment.yaml', '--wait=true')
    return {'replicas_up': len(five), 'replicas_down': len(two),
            'balanced_pods_at_5': hit_pods, 'declarative_replicas': len(three),
            'balanced_pods_at_3': hit_three, 'http_samples': len(samples),
            'http_errors': sum(not s['ok'] for s in samples),
            'versions_observed': sorted(set(re.findall(r'v=\d+', ' '.join(s.get('response', '') for s in samples)))),
            'bad_image_reasons': errors, 'rollback_v2': True, 'rollback_v1': True}


def lab2():
    ns = 'lab-configmap-secret'
    apply('lab-2/namespace.yaml')
    k('-n', ns, 'create', 'configmap', 'pipeline-config', '--from-file=lab-2/pipeline-config.properties')
    k('-n', ns, 'create', 'configmap', 'data-paths', '--from-literal=bronze=/mnt/data/bronze',
      '--from-literal=silver=/mnt/data/silver', '--from-literal=gold=/mnt/data/gold')
    apply('lab-2/configmap-data-platform.yaml')
    for name in ('pipeline-config', 'data-paths', 'data-platform-config'):
        k('-n', ns, 'get', 'configmap', name, '-o', 'yaml')
    demo_access = 'AKIAIOSFODNN7EXAMPLE'
    demo_secret = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY'
    k('-n', ns, 'create', 'secret', 'generic', 'ceph-credentials',
      '--from-literal=access-key=' + demo_access, '--from-literal=secret-key=' + demo_secret)
    k('-n', ns, 'create', 'secret', 'generic', 's3-credentials', '--from-file=lab-2/s3-credentials.txt')
    apply('lab-2/secret-ceph-credentials.yaml')
    for name in ('ceph-credentials', 's3-credentials', 'ceph-s3-credentials'):
        k('-n', ns, 'get', 'secret', name, '-o', 'yaml')
    secret = obj('-n', ns, 'get', 'secret', 'ceph-s3-credentials')
    decoded = {key: base64.b64decode(value).decode() for key, value in secret['data'].items()}
    assert decoded['access-key'] == demo_access and decoded['secret-key'] == demo_secret
    record('Décodage des valeurs fictives du cours: ' + json.dumps(decoded))
    for filename in ('pod-with-configmap.yaml', 'pod-with-secret.yaml',
                     'pod-with-configmap-volume.yaml', 'pod-with-secret-volume.yaml'):
        apply('lab-2/' + filename)
    for pod in ('spark-app', 'spark-config-reader', 'ceph-connector-secure'):
        k('-n', ns, 'wait', '--for=condition=Ready', 'pod/' + pod, '--timeout=180s')
    k('-n', ns, 'wait', '--for=jsonpath={.status.phase}=Succeeded', 'pod/ceph-connector', '--timeout=180s')
    env_output = k('-n', ns, 'exec', 'spark-app', '--', 'env')
    assert 'LOG_LEVEL=INFO' in env_output and 'SPARK_WORKERS=4' in env_output
    access_log = k('-n', ns, 'logs', 'ceph-connector')
    assert demo_access in access_log
    config = obj('-n', ns, 'get', 'configmap', 'data-platform-config')['data']
    for key, filename in [('application.yaml', 'application.yaml'), ('table_mappings.csv', 'tables.csv')]:
        mounted = k('-n', ns, 'exec', 'spark-config-reader', '--', 'cat', '/etc/config/' + filename)
        assert mounted == config[key]
    k('-n', ns, 'exec', 'spark-config-reader', '--', 'ls', '-la', '/etc/config')
    k('-n', ns, 'exec', 'ceph-connector-secure', '--', 'ls', '-laL', '/etc/ceph-credentials')
    mode = k('-n', ns, 'exec', 'ceph-connector-secure', '--', 'stat', '-L', '-c', '%a',
             '/etc/ceph-credentials/s3-credentials.txt').strip()
    assert mode == '600'
    credentials = k('-n', ns, 'exec', 'ceph-connector-secure', '--', 'cat',
                    '/etc/ceph-credentials/s3-credentials.txt')
    assert credentials == (ROOT / 'lab-2/s3-credentials.txt').read_text()
    k('delete', 'namespace', ns, '--wait=true')
    assert ns not in [item['metadata']['name'] for item in obj('get', 'namespaces')['items']]
    return {'configmaps': 3, 'secrets': 3, 'env_configmap_verified': True,
            'env_secret_verified': True, 'decoded_secret_verified': True,
            'mounted_config_files_verified': 2, 'mounted_secret_file_verified': True,
            'secret_file_mode': mode, 'namespace_deleted': True,
            'credentials': 'Exemples fictifs fournis par le cours ; aucun accès Ceph/S3 réel.'}


def failure_reason(ns, name):
    k('-n', ns, 'wait', '--for=condition=Failed', 'job/' + name, '--timeout=330s', timeout=360)
    job = obj('-n', ns, 'get', 'job', name)
    k('-n', ns, 'describe', 'job', name)
    k('-n', ns, 'get', 'pods', '-l', 'job-name=' + name, '-o', 'wide')
    k('-n', ns, 'logs', '-l', 'job-name=' + name, '--prefix=true', '--tail=-1')
    return next(c['reason'] for c in job['status']['conditions'] if c['type'] == 'Failed')


def lab3():
    ns = 'lab-job-cronjob'
    run(MK + ['image', 'pull', 'python:3.13-slim'])
    apply('lab-3/namespace.yaml')
    apply('lab-3/job-simple.yaml')
    apply('lab-3/job-with-retry.yaml')
    apply('lab-3/job-with-deadline.yaml')
    apply('lab-3/job-parallel.yaml')
    apply('lab-3/configmap-etl.yaml')
    for filename in ('cronjob-pipeline.yaml', 'cronjob-backup.yaml', 'cronjob-etl.yaml'):
        apply('lab-3/' + filename)
    k('-n', ns, 'wait', '--for=condition=Complete', 'job/data-import-job', '--timeout=120s')
    simple_logs = k('-n', ns, 'logs', 'job/data-import-job')
    assert 'Import completed!' in simple_logs
    k('-n', ns, 'wait', '--for=condition=Complete', 'job/batch-export-job', '--timeout=120s')
    parallel = obj('-n', ns, 'get', 'job', 'batch-export-job')
    assert parallel['status']['succeeded'] == 4
    parallel_logs = k('-n', ns, 'logs', '-l', 'job-name=batch-export-job', '--prefix=true', '--tail=-1')
    shards = sorted(set(map(int, re.findall(r'Shard (\d) export completed!', parallel_logs))))
    assert shards == [0, 1, 2, 3]
    deadline_reason = failure_reason(ns, 'data-transform-deadline-job')
    assert deadline_reason == 'DeadlineExceeded'
    retry_reason = failure_reason(ns, 'data-transform-job')
    assert retry_reason == 'BackoffLimitExceeded'
    retry_pods = obj('-n', ns, 'get', 'pods', '-l', 'job-name=data-transform-job')['items']
    assert len(retry_pods) == 4
    k('-n', ns, 'create', 'job', 'etl-manual-run', '--from=cronjob/etl-bronze-to-silver')
    k('-n', ns, 'wait', '--for=condition=Complete', 'job/etl-manual-run', '--timeout=120s')
    etl_logs = k('-n', ns, 'logs', 'job/etl-manual-run')
    assert 'ETL: bronze -> silver' in etl_logs and 'Batch size: 1000' in etl_logs
    backup = obj('-n', ns, 'get', 'cronjob', 'daily-backup')
    assert backup['spec']['schedule'] == '0 2 * * *' and backup['spec']['timeZone'] == 'Europe/Paris'
    k('-n', ns, 'create', 'job', 'backup-manual-run', '--from=cronjob/daily-backup')
    k('-n', ns, 'wait', '--for=condition=Complete', 'job/backup-manual-run', '--timeout=120s')
    assert 'Backup completed' in k('-n', ns, 'logs', 'job/backup-manual-run')
    # Observe une véritable exécution planifiée du CronJob toutes les 2 minutes.
    deadline = time.monotonic() + 180
    scheduled = None
    while time.monotonic() < deadline:
        jobs = obj('-n', ns, 'get', 'jobs')['items']
        scheduled = next((j for j in jobs if any(o.get('kind') == 'CronJob' and
                          o['name'] == 'bronze-to-silver-pipeline'
                          for o in j['metadata'].get('ownerReferences', []))), None)
        if scheduled:
            break
        time.sleep(3)
    assert scheduled, 'Aucun Job planifié observé'
    scheduled_name = scheduled['metadata']['name']
    k('-n', ns, 'wait', '--for=condition=Complete', 'job/' + scheduled_name, '--timeout=120s')
    scheduled_logs = k('-n', ns, 'logs', 'job/' + scheduled_name)
    assert '500 records processed' in scheduled_logs
    k('-n', ns, 'patch', 'cronjob', 'bronze-to-silver-pipeline', '--type=merge',
      '-p', '{"spec":{"suspend":true}}')
    assert obj('-n', ns, 'get', 'cronjob', 'bronze-to-silver-pipeline')['spec']['suspend'] is True
    k('-n', ns, 'patch', 'cronjob', 'bronze-to-silver-pipeline', '--type=merge',
      '-p', '{"spec":{"suspend":false}}')
    assert obj('-n', ns, 'get', 'cronjob', 'bronze-to-silver-pipeline')['spec']['suspend'] is False
    k('-n', ns, 'get', 'jobs,cronjobs,pods', '-o', 'wide')
    k('delete', 'namespace', ns, '--wait=true')
    assert ns not in [item['metadata']['name'] for item in obj('get', 'namespaces')['items']]
    return {'simple_job': 'Complete', 'retry_pods': len(retry_pods),
            'retry_failure_reason': retry_reason, 'deadline_failure_reason': deadline_reason,
            'parallel_succeeded': 4, 'completed_indexes': parallel['status']['completedIndexes'],
            'shards': shards, 'manual_etl': 'Complete', 'etl_batch_size': 1000,
            'manual_backup': 'Complete', 'backup_timezone': 'Europe/Paris',
            'scheduled_job': scheduled_name, 'scheduled_job_complete': True,
            'suspend_resume_verified': True, 'namespace_deleted': True,
            'data_processing': 'Simulations Python du cours, sans stockage externe.'}


def main():
    global LOG
    chosen = sys.argv[1] if len(sys.argv) > 1 else 'all'
    if chosen not in ('1', '2', '3', 'all'):
        raise SystemExit('Usage: python -X utf8 run_labs.py 1|2|3|all')
    for number in (1, 2, 3) if chosen == 'all' else (int(chosen),):
        print(f'Lab {number}: exécution...', flush=True)
        with (RESULTS / f'lab-{number}.txt').open('w', encoding='utf-8') as LOG:
            (RESULTS / f'lab-{number}.json').unlink(missing_ok=True)
            started = datetime.now(timezone.utc).isoformat()
            record('Exécution réelle UTC: ' + started)
            try:
                checks = globals()[f'lab{number}']()
            except BaseException as exc:
                record('ÉCHEC: ' + repr(exc))
                raise
            result = {'lab': number, 'started_at': started,
                      'finished_at': datetime.now(timezone.utc).isoformat(),
                      'status': 'passed', 'checks': checks}
            (RESULTS / f'lab-{number}.json').write_text(
                json.dumps(result, indent=2, ensure_ascii=False) + '\n', encoding='utf-8')
            print(json.dumps(result, ensure_ascii=False), flush=True)


if __name__ == '__main__':
    main()
