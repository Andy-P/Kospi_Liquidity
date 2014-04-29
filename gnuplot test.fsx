open System
open System.Diagnostics

let path = @"Applications/Gnuplot.app/Contents/Resources/bin/gnuplot"  

let gp =
  new ProcessStartInfo
    (FileName = path, UseShellExecute = false, 
     CreateNoWindow = true, RedirectStandardInput = true) 
  |> Process.Start


gp.StandardInput.WriteLine "plot sin(x) + sin(3*x), -x"

