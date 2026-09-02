# Launch the full parse as parallel, detached lanes (one python per lane,
# balanced by compressed size). Each lane resumes: parsed payers are skipped.
# Logs: lane_<n>.log. Survives closing this terminal.
$lanes = @(
  @("UHC_NY_NationalPPO"),
  @("UHC_NY_ChoiceEPO50"),
  @("UHC_NY_POSChoicePlus"),
  @("UHC_NY_ChoicePlus", "Cigna_NationalOAP", "Cigna_PathwellPPO"),
  @("UHC_NY_ChoiceEPO",  "Cigna_NationalPPO", "Cigna_LocalPlus"),
  @("UHC_NY_SelectEPO",  "AetnaALIC_OpenAccessElectChoice"),
  @("AetnaALIC_OpenAccessManagedChoice", "AetnaALIC_Epo", "AetnaALIC_Ppo",
    "AetnaALIC_OpenAccessHealthNetworkOption", "AetnaALIC_Hmo")
)
$i = 0
foreach ($lane in $lanes) {
  $i++
  $args = @("run_pipeline.py", "--only") + $lane
  Start-Process -FilePath "python" -ArgumentList $args -WorkingDirectory $PSScriptRoot `
    -WindowStyle Hidden -RedirectStandardOutput "lane_$i.log" -RedirectStandardError "lane_$i.err"
  Write-Output ("lane {0}: {1}" -f $i, ($lane -join ", "))
}
