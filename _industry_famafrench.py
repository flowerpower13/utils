

#imports
import re


#https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html


def parse_ranges(lines):
    ranges_with_labels = []
    current_label = None

    for line in lines:
        line = line.strip()

        if line:
            label_match = re.match(r'^\s*(\d+)\s+(\w+)\s+', line)
            range_match = re.match(r'^\s*(\d+)-(\d+)', line)

            if label_match:
                current_label = label_match.group(1)
            elif range_match and current_label is not None:
                start_range = int(range_match.group(1))
                end_range = int(range_match.group(2))
                ranges_with_labels.append((range(start_range, end_range + 1), current_label))

    return ranges_with_labels


def _filepath_to_mapping(filepath):

    #read
    with open(filepath, "r") as file:
        lines = file.readlines()

    #range
    ranges_with_labels = parse_ranges(lines)

    #mapping
    sic_to_ff = {r: label for r, label in ranges_with_labels}

    #return
    return sic_to_ff


