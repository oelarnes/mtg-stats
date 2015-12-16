def str_reverse(s):
    char_list = [s[-j-1] for j in range(len(s))]
    return ''.join(char_list)

def standardize_name(name):
    first_last = name.split(' ')
    return ' '.join(first_last[1:]) + ', ' + first_last[0]

def indexOf(li, el):
  try:
    return li.index(el)
  except:
    return None
