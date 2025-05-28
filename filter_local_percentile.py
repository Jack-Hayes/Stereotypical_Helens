import sys
import numpy as np

import sys
import numpy as np

def filter_local_percentile(ins, outs):
    """
    Approximate RH_x by sampling up to 10k points for percentile estimation,
    then apply that cutoff to the full ins['Z'] array.
    Uses ins.dtype.names to iterate over all dimensions correctly.
    """
    try:
        # Read threshold from pdalargs global
        # Only two arguments -- ins and outs numpy arrays -- can be passed!
        # TODO: fix hardcoded 98
        thr = float(pdalargs.get("percentile_threshold", 0.98))
        z = ins["Z"]

        # Sample up to N points for speed
        # np.nanpercentile on multi-million‐length arrays is O(n log n) and memory‐heavy
        # PDAL’s Python subprocess will appear to “freeze” while that runs
        N_sample = 10_000
        if len(z) > N_sample:
            indices = np.random.choice(len(z), size=N_sample, replace=False)
            sample = z[indices]
        else:
            sample = z

        # Estimate cutoff on sample
        cutoff = np.percentile(sample, thr * 100)

        # 3Apply to full array
        mask = z >= cutoff

        # Copy all dimensions by dict keys (ins is a dict of arrays)
        for field, arr in ins.items():
            outs[field] = arr[mask]

        sys.stderr.write(
            f"[filter_local_pct] thr={thr}, cutoff={cutoff:.2f}, "
            f"orig={len(z)}, kept={mask.sum()}\n"
        )
        return True

    except Exception:
        # Print full traceback so PDAL STDERR shows what went wrong
        import traceback
        traceback.print_exc(file=sys.stderr)
        raise

