import time
import math
import random
import bisect
import multiprocessing
import matplotlib.pyplot as plt


def sequential_merge_sort(input_list):
    """
    Sequential merge sort
    :param input_list: list
    :return: sorted list
    """
    if len(input_list) <= 1:
        return input_list
    middle = len(input_list) // 2
    left, right = input_list[:middle], input_list[middle:]
    left = sequential_merge_sort(left)
    right = sequential_merge_sort(right)
    return sequential_merge(left, right)


def sequential_merge(left, right):
    """
    Performs merge of two sorted lists
    :param left: list
    :param right: list
    :return: merged list
    """
    result = []
    left_size = len(left)
    right_size = len(right)
    i, j = 0, 0
    while i != left_size or j != right_size:
        if i != left_size and (j == right_size or left[i] < right[j]):
            result.append(left[i])
            i += 1
        else:
            result.append(right[j])
            j += 1
    return result


def _merger(shared_array, level, num, proc, verbose=False):
    """
    Main worker of parallel merge sort
    :param shared_array: shared multiprocessing array
    :param level: level
    :param num: number of core
    :param proc: processing field
    :return:
    """
    lbound = proc * num
    rbound = proc * (num + 1)
    if verbose:
        print('Upper Worker ' + str(num) + ' ' + str(lbound) + ' ' + str(rbound))
    for i in range(lbound + 2**level, rbound, 2**(level + 1)):
        # left list: [i - 2**level, i-1]
        # right list: [i, i + 2**level - 1]
        shared_array[i - 2 ** level: i + 2 ** level] = _merge_with_binary_search(
            shared_array[i - 2 ** level: i],
            shared_array[i: i + 2 ** level],
            2**(level + 1)
        )


def _merge_with_binary_search(list_1, list_2, output_size):
    """
    Perform merge using binary search to find position of
    element in result array
    :param list_1: list
    :param list_2: list
    :param output_size: size of merged list
    :return: merged list
    """
    merged_list = [None] * output_size
    for i in range(len(list_1)):
        # binary search
        rank_2 = bisect.bisect_left(list_2, list_1[i])
        merged_list[i + rank_2] = list_1[i]

    # probably this is not ok to have loop here
    for j in range(len(merged_list)):
        if merged_list[j] is None:
            merged_list[j] = list_2.pop(0)
    return merged_list


def _get_core_and_processing_field(length, level, max_cores):
    """
    Get core number and processing field for given level
    :param length: size of input array
    :param level: level
    :return: core number and processing field
    """
    processing_field = 2**(level + 1)
    core_number = int(length / processing_field)
    if core_number > max_cores:
        core_number = max_cores
        processing_field = int(length / core_number)
    return core_number, processing_field


def parallel_merge_sort(input, max_cores):
    """
    Performs parallel merge sort multiple processes
    :param input: list of input elements
    :param max_cores: maximum number of cores
    :return: sorted list
    """
    jobs = []
    length = len(input)
    shared_array = multiprocessing.Array('i', input)
    depth = math.log(length, 2)
    for level in range(0, int(depth)):
        # print("Level:", level)
        core_number, processing_field = _get_core_and_processing_field(length, level, max_cores)
        for i in range(core_number):
            p = multiprocessing.Process(target=_merger, args=(shared_array, level, i, processing_field))
            p.daemon = False
            jobs.append(p)
            p.start()
        for p in jobs:
            p.join()
    return shared_array


if __name__ == '__main__':
    max_cores = 4
    # input_list = [random.randint(-10000, 10000) for i in range(2**3)]
    # print(sequential_merge_sort(input_list))
    # print(input_list)
    # print(parallel_merge_sort(input_list, max_cores)[:])
    sequential_execution_times = []
    parallel_execution_times = []
    input_list_sizes = []
    for i in range(18):
        list_size = 2 ** i
        input_list_sizes.append(i)

        input_list = [random.randint(-10000, 10000) for i in range(list_size)]

        start = time.time()
        parallel_merge_sort(input_list, max_cores)
        end = time.time() - start
        parallel_execution_times.append(end)
        print("Parallel merge sort with list of size {0} execution time: {1}".format(list_size, end))

        start = time.time()
        sequential_merge_sort(input_list)
        end = time.time() - start
        sequential_execution_times.append(end)
        print("Sequential merge sort  with list of size {0} execution time: {1}".format(list_size, end))

    plt.xlabel("Size of array 2^x")
    plt.ylabel("Execution time")
    plt.plot(input_list_sizes, sequential_execution_times)
    plt.plot(input_list_sizes, parallel_execution_times)
    plt.legend(["Sequential", "Parallel"], loc=2)
    plt.show()
