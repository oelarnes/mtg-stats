def str_reverse(s):
    char_list = [s[-j-1] for j in range(len(s))]
    return ''.join(char_list)
