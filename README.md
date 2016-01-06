# luigi-cern
Luigi modules for working with the LHC grid

To run the example:
 1. Make sure that XRootD is installed
 2. `pip install -r requirements.txt`
 3. Make sure you are authenticated via Kerberos
 4. Launch `luigid` in the background
 5. Adjust the `EOS_HOME` and `DEFAULT_SERVER` in `example.py`
 6. Run `python example.py SumNumbers --outpath=/eos/lhcb/user/i/ibabusch/summed`
    with your own EOS output path

