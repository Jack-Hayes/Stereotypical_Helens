import sys
import numpy as np
import scipy.stats as stats

def filter_percentile(ins, outs):
    """
    Trims extreme Z outliers via Z-score thresholding.
    Uses the pdalargs global for 'percentile_threshold'.
    """
    try:
        # pdalargs is injected by PDAL into this module's globals
        # Only two arguments -- ins and outs numpy arrays -- can be passed!
        thr = float(pdalargs.get("percentile_threshold", 0.95))
        z_val = stats.norm.ppf(thr)

        z = ins["Z"]
        m = np.nanmean(z)
        s = np.nanstd(z)
        zs = (z - m) / s

        newclass = np.where(zs > z_val, 18, ins["Classification"])
        outs["Classification"] = newclass

        # Debug print to stderr
        sys.stderr.write(
            f"[filter_percentile] thr={thr}, z_val={z_val:.2f}, "
            f"orig={len(z)}, kept={(zs <= z_val).sum()}\n"
        )
        return True

    except Exception as e:
        # Raise so PDAL prints stacktrace instead of hanging
        raise RuntimeError(f"filter_percentile error: {e}")
