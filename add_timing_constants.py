import ROOT
import numpy as np
import uproot
import awkward as ak
import gc
import tracemalloc
import argparse
import os
import time
from analysis_tools import CalibrationDBInterface

def add_timing_constants(input_file_names, run_number ,output_dir):
    # get the timing constants from calibration database
    calibration_db_interface = CalibrationDBInterface()
    timing_offsets_list = calibration_db_interface.get_calibration_constants(run_number, 0, "timing_offsets", 0)
    timing_offsets_dict = {}
    #load into dict
    for offset in timing_offsets_list:
        timing_offsets_dict[offset['position_id']]=offset['timing_offset']

    # make a fast lookup table for the offsets
    # Define a safe lookup function with default fallback
    DEFAULT_OFFSET = 0
    def safe_lookup(db_pmt_id):
        return timing_offsets_dict.get(db_pmt_id, DEFAULT_OFFSET)
    timing_offset_lookup = np.frompyfunc(safe_lookup, 1, 1)

    # Vectorized check whether a constant was found function
    def has_constant(db_pmt_id):
        return db_pmt_id in timing_offsets_dict
    has_time_constant_lookup = np.frompyfunc(has_constant, 1, 1)
        
    tree_name = "WCTEReadoutWindows"  # Replace with actual TTree name

    for input_file_name in input_file_names:
        # Construct output path
        filename = os.path.basename(input_file_name)
        os.makedirs(output_dir, exist_ok=True)
        output_file = os.path.join(output_dir, filename)

        input_file = ROOT.TFile.Open(input_file_name)
        tree = input_file.Get(tree_name)
        
        output_file = ROOT.TFile(output_file, "RECREATE")
        out_tree = tree.CloneTree(0)  # clone structure only, no entries

        hit_pmt_calibrated_times = ROOT.std.vector('double')()
        hit_pmt_calibrated_times_branch = out_tree.Branch("hit_pmt_calibrated_times", hit_pmt_calibrated_times)

        hit_pmt_has_time_constant = ROOT.std.vector('bool')()
        hit_pmt_has_time_constant_branch = out_tree.Branch("hit_pmt_has_time_constant", hit_pmt_has_time_constant)

        for i, entry in enumerate(tree):
            hit_pmt_calibrated_times.clear()
            hit_pmt_has_time_constant.clear()

            if i%10_000==0:
                print("On event",i)

            hit_times = np.array(list(entry.hit_pmt_times))
            hit_mpmt_slot = np.array(list(entry.hit_mpmt_slot_ids))
            hit_pmt_pos = np.array(list(entry.hit_pmt_position_ids))
            db_pmt_id = hit_mpmt_slot * 100 + hit_pmt_pos
            
            timing_offsets = timing_offset_lookup(db_pmt_id)
            calibrated_times = hit_times - timing_offsets
            
            has_time_constant = has_time_constant_lookup(db_pmt_id)
            for time, flag in zip(calibrated_times,has_time_constant):
                hit_pmt_calibrated_times.push_back(float(time))
                hit_pmt_has_time_constant.push_back(bool(flag))
            
            out_tree.Fill()

        out_tree.Write()
        output_file.Close()
        input_file.Close()

    print(f"Finished writing output to: {output_file}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Add a new branch to a ROOT TTree in batches.")
    parser.add_argument("-i","--input_files",nargs='+', help="Path to input ROOT file or files")
    parser.add_argument("-r","--run_number",nargs='+', help="Run Number")
    parser.add_argument("-o","--output_dir", help="Directory to write output file")

    args = parser.parse_args()
    start = time.time()
    add_timing_constants(args.input_files, args.run_number, args.output_dir)
    end = time.time()
    print(f"Elapsed time: {end - start:.3f} seconds")
    # add_timing_constants(["/eos/experiment/wcte/data/2025_commissioning/offline_data_vme_match/WCTE_offline_R2370S0_VME2005.root"], 2370, "/afs/cern.ch/user/l/lcook/user_data/test")
