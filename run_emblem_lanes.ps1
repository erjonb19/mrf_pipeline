# Launch the remaining Emblem payers as parallel, detached lanes.
#
# Emblem publishes ~280 files of ~11 MB each rather than a handful of huge
# ones, so the sequential run spends most of its time waiting on HTTP rather
# than parsing. That is the case lanes are for: one python per lane, each with
# a disjoint slice of the queue.
#
# The queue is computed here, not hardcoded, so this is re-runnable -- a payer
# already on disk is dropped before the split, and a crashed lane's leftovers
# are picked up by the next invocation instead of needing a hand-edited list.
#
# Lane count is bounded by RAM, not cores: these parses are small, but each
# lane is a whole interpreter. Logs: emblem_lane_<n>.log
param([int]$Lanes = 6)

$py = "C:\Users\Erjon\AppData\Local\Programs\Python\Python311\python.exe"

# Disjoint slices matter: two processes on one payer write the same .part file
# and corrupt it. Round-robin is safe here because every Emblem file is about
# the same size, so the lanes finish together.
$remaining = & $py -c @"
import os, config
print('\n'.join(
    pf['payer'] for pf in config.PAYER_FILES
    if pf['payer'].startswith('Emblem')
    and not os.path.exists(os.path.join('payer_parquet', pf['payer'] + '.parquet'))))
"@

$remaining = @($remaining | Where-Object { $_ })
if ($remaining.Count -eq 0) { Write-Output "nothing left to parse"; exit 0 }
if ($Lanes -gt $remaining.Count) { $Lanes = $remaining.Count }

Write-Output ("{0} Emblem payers remaining, {1} lanes" -f $remaining.Count, $Lanes)

for ($i = 0; $i -lt $Lanes; $i++) {
  $slice = @()
  for ($j = $i; $j -lt $remaining.Count; $j += $Lanes) { $slice += $remaining[$j] }
  $n = $i + 1
  Start-Process -FilePath $py `
    -ArgumentList (@("-u", "run_pipeline.py", "--only") + $slice) `
    -WorkingDirectory $PSScriptRoot -WindowStyle Hidden `
    -RedirectStandardOutput "emblem_lane_$n.log" -RedirectStandardError "emblem_lane_$n.err"
  Write-Output ("  lane {0}: {1} payers ({2} ...)" -f $n, $slice.Count, $slice[0])
}
