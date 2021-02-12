import bisect
from typing import AnyStr, List


def get_closest_configs_bisect(timestamp: AnyStr, timestamps: List[AnyStr]):
    """
    Get the closest matching configuration name in an array.

    Configuration names are timestamps as strings.

    :timestamp (AnyStr) A string representing the name of the configuration.
    :timestamps (List[AnyStr]) A list containing all the configuration names.

    Return the index of the closest matching configuration in the timestamps array.
    """
    timestamps_len = len(timestamps)
    if timestamps_len == 1:
        return 0
    index = bisect.bisect_left(timestamps, timestamp)
    if index == timestamps_len:
        return index - 1
    return index