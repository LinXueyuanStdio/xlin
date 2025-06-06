
def stream_average(avg_pre: float, cur_num_index: int, cur_num: float) -> float:
    """
    calculate the average of number in a stream
    given nums: List[float], calculate avg[n] = sum(nums[:n]) / len(nums[:n]) = sum(nums[:n]) / n
    however, we don't know the length of the stream, so we use a moving average
    so avg[n-1] = sum(nums[:n-1]) / (n-1)
    so n * avg[n] = (n-1) * avg[n-1] + nums[n]
    so avg[n] = avg[n-1] + (nums[n] - avg[n-1]) / n
    we don't need to remember every nums before n

    Usage:
    >>> acc = 0
    >>> nums = [1, 0, 1, 0, 0, 1, 1, 1, 0, 1]
    >>> for i, num in enumerate(nums):
    ...     acc = stream_average(acc, i, num)

    :param avg_pre: (initially 0) average of previous numbers from index 0 to cur_num_index-1, i.e. avg[n-1]
    :param cur_num_index: current number index starting from 0, i.e. n
    :param cur_num: current number, i.e. nums[n]
    :return: average of current number and previous numbers, i.e. avg[n]
    """
    return avg_pre + (cur_num - avg_pre) / (cur_num_index + 1)


class AverageMeter:
    """Computes and stores the average and current value"""

    def __init__(self, name, fmt=':f'):
        self.name = name
        self.fmt = fmt
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0
        self.reset()

    def reset(self):
        self.val = 0
        self.avg = 0
        self.sum = 0
        self.count = 0

    def update(self, val, n=1):
        self.val = val
        self.sum += val * n
        self.count += n
        self.avg = self.sum / self.count

    def __str__(self):
        fmtstr = '{name} {val' + self.fmt + '} ({avg' + self.fmt + '})'
        return fmtstr.format(**self.__dict__)

