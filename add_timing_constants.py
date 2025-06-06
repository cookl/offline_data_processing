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
    
    
    # branches_to_load = ["hit_pmt_times", "hit_mpmt_slot_ids", "hit_pmt_position_ids"]
    
    # for istep, arrays in enumerate(uproot.iterate(f"{input_file}:{tree_name}", step_size=10, library="ak", branches=branches_to_load)):
    #     if istep == 0:
    #         tracemalloc.start()
    #     if istep>10:
    #         break
    #     current, peak = tracemalloc.get_traced_memory()
    #     print(f"Step {istep} memory usage: current={current / 1e6:.2f} MB, peak={peak / 1e6:.2f} MB")

    # # arrays contains all branches by default
    #     print("Step",istep)
    #     hit_times = arrays["hit_pmt_times"]
    #     slot_ids = arrays["hit_mpmt_slot_ids"]
    #     pos_ids = arrays["hit_pmt_position_ids"]

    #     #the lookup id for the calibraiton database
    #     db_pmt_id = slot_ids * 100 + pos_ids

    #     #flatten for fast lookup
    #     db_pmt_id_flat = ak.to_numpy(ak.flatten(db_pmt_id))

    #     print("Applying timing correction")
    #    # Apply vectorized dictionary lookup (handles negatives, missing keys)
    #     timing_offsets_flat = timing_offset_lookup(db_pmt_id_flat)
    #     timing_offsets_flat = np.array(timing_offsets_flat, dtype=float)
    #     timing_offsets = ak.unflatten(timing_offsets_flat, ak.num(db_pmt_id))
    #     # timing_offsets may contain None where missing; replace with default
    #     # timing_offsets = ak.fill_none(timing_offsets, DEFAULT_OFFSET)

    #     calibrated_times = hit_times - timing_offsets
        
    #     print("getting has time constant")
    #     # has_time_constant = ak.Array(has_constant_lookup(ak.to_numpy(db_pmt_id)))
    #     has_time_constant_flat = has_time_constant_lookup(db_pmt_id_flat)
    #     has_time_constant_flat = np.array(has_time_constant_flat, dtype=bool)
    #     has_time_constant = ak.unflatten(has_time_constant_flat, ak.num(db_pmt_id))
        
    #     print("Putting back in array")

    #     arrays["hit_pmt_calibrated_times"] = calibrated_times
    #     arrays["hit_pmt_has_time_constant"] = has_time_constant
        
    #     with uproot.recreate(output_file) as fout:
    #         if "WCTEReadoutWindows" in fout:
    #             print("Tree found!")
    #         else:
    #             print("Tree NOT found!")
    #             print(len(arrays),arrays)
    #             fout["WCTEReadoutWindows"] = arrays[0]
            
    #     # with uproot.update(output_file) as fout:
    #     #     fout[tree_name].extend(arrays)

    #     # Append to output file
    #     # with uproot.update(output_file) as fout:
        
    #     #     fout[tree_name].extend(arrays)
    #     print("force cleanup")
    #     del arrays, hit_times, slot_ids, pos_ids, db_pmt_id, db_pmt_id_flat
    #     del timing_offsets_flat, timing_offsets, calibrated_times
    #     del has_time_constant_flat, has_time_constant
    #     gc.collect()

    #     print("finished cleanup")

        
    print(f"Finished writing output to: {output_file}")
    
# input_file = "input.root"
# tree_name = "tree_name"
# output_file = "output.root"
# new_branch_name = "new_vector_branch"

# # Step 1: Create output ROOT file (empty)
# with uproot.recreate(output_file):
#     pass

# # Step 2: Iterate in batches
# for arrays in uproot.iterate(f"{input_file}:{tree_name}", step_size=100_000, library="ak"):
#     # arrays is an awkward.Record or awkward.Array with jagged structures

#     # For demonstration: Make a new vector branch with the same length structure as an existing one
#     # Pick an existing vector branch to match the offsets
#     reference_vector = arrays["some_existing_vector_branch"]  # e.g. std::vector<float>

#     # Create new vector values (example: a vector of same length per entry, filled with zeros)
#     # Match the nested structure
#     new_vector = ak.ones_like(reference_vector) * 0.0  # Replace with your actual logic

#     # Add new branch to arrays
#     arrays[new_branch_name] = new_vector

#     # Append to output file
#     with uproot.update(output_file) as fout:
#         fout[tree_name].extend(arrays)

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
