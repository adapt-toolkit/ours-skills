#!/usr/bin/env python3
"""Offline CPU embedding worker. Exact local artifacts; no HTTP or Hindsight calls."""
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

from context_budget import compact_json


def binding(cfg):
    settings=cfg.get('note_embedding')
    if settings is None:return None
    keys={'python','model','tokenizer','model_sha256','tokenizer_sha256'}
    if not isinstance(settings,dict) or set(settings)!=keys:raise ValueError('offline operator embedding binding required')
    for key in ('python','model','tokenizer'):
        if not Path(settings[key]).is_absolute():raise ValueError('absolute local embedding artifact required')
    for key in ('model_sha256','tokenizer_sha256'):
        v=settings[key]
        if not isinstance(v,str) or len(v)!=64 or any(c not in '0123456789abcdef' for c in v):
            raise ValueError('pinned embedding artifact digest required')
    def embed(kind,texts):
        if kind not in ('query','passage') or not isinstance(texts,list) or not 1<=len(texts)<=3:
            raise ValueError('bounded local embedding batch')
        payload=compact_json({'settings':settings,'kind':kind,'texts':texts})
        # No user message is placed in argv/environment or a disk artifact.
        env={'PATH':'/usr/bin:/bin','HF_HUB_OFFLINE':'1','TRANSFORMERS_OFFLINE':'1','TOKENIZERS_PARALLELISM':'false',
             'OMP_NUM_THREADS':'2','OPENBLAS_NUM_THREADS':'2','PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'}
        try:
            child=subprocess.run([settings['python'],str(Path(__file__).absolute()),'--worker'],
                input=payload.encode(),stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,env=env,timeout=10,check=False)
        except subprocess.TimeoutExpired:raise TimeoutError('offline embedding deadline') from None
        if child.returncode:raise OSError('offline embedding unavailable')
        result=json.loads(child.stdout)
        if set(result)!={'model','vectors'} or len(result['vectors'])!=len(texts):raise ValueError('invalid local embedding envelope')
        expected=hashlib.sha256((settings['model_sha256']+settings['tokenizer_sha256']+'e5-mean-l2-512-v1').encode()).hexdigest()
        if result['model']!=expected:raise ValueError('offline embedding identity mismatch')
        return result['model'],result['vectors']
    return embed


def worker(request):
    # Direct Tokenizer/ORT local files. No hub client, remote extractor, raw note logging or shared bank.
    import numpy as np
    import onnxruntime as ort
    from tokenizers import Tokenizer
    settings=request['settings'];texts=request['texts'];kind=request['kind']
    if kind not in ('query','passage') or not isinstance(texts,list) or not 1<=len(texts)<=3:
        raise ValueError('invalid embedding request')
    for key in ('model','tokenizer'):
        with Path(settings[key]).open('rb') as f:
            h=hashlib.file_digest(f,'sha256').hexdigest()
        if h!=settings[key+'_sha256']:raise ValueError('model artifact changed')
    tokenizer=Tokenizer.from_file(settings['tokenizer'])
    tokenizer.no_truncation()  # Complete summary reaches the pinned model.
    encoded=tokenizer.encode_batch([kind+': '+t for t in texts])
    if any(not isinstance(t,str) or not t.strip() for t in texts):raise ValueError('bounded embedding text required')
    width=max(len(t.ids) for t in encoded)
    ids=np.array([t.ids+[1]*(width-len(t.ids)) for t in encoded],dtype=np.int64)
    mask=np.array([[1]*len(t.ids)+[0]*(width-len(t.ids)) for t in encoded],dtype=np.int64)
    opts=ort.SessionOptions();opts.intra_op_num_threads=2;opts.inter_op_num_threads=1;opts.enable_cpu_mem_arena=False
    session=ort.InferenceSession(settings['model'],sess_options=opts,providers=['CPUExecutionProvider'])
    inputs={'input_ids':ids,'attention_mask':mask,'token_type_ids':np.zeros_like(ids)}
    output=session.run(None,{i.name:inputs[i.name] for i in session.get_inputs()})[0]
    if output.ndim!=3:raise ValueError('expected E5 token embeddings')
    pooled=(output*mask[...,None]).sum(axis=1)/mask.sum(axis=1)[:,None]
    pooled=pooled/np.linalg.norm(pooled,axis=1,keepdims=True)
    if pooled.shape!=(len(texts),384) or not np.isfinite(pooled).all():raise ValueError('invalid offline vectors')
    model=hashlib.sha256((settings['model_sha256']+settings['tokenizer_sha256']+'e5-mean-l2-512-v1').encode()).hexdigest()
    return {'model':model,'vectors':pooled.tolist()}


if __name__=='__main__':
    try:
        raw=sys.stdin.buffer.read()
        print(compact_json(worker(json.loads(raw))))
    except Exception:
        sys.exit(1)
