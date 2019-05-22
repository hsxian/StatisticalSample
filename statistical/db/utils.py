def pw_lst_2_py_dic(pw_lst):
    if isinstance(pw_lst, list):
        dics = dict()
        for obj in pw_lst:
            dics[str(obj.id)] = str(obj.name)
            dics[str(obj.name)] = str(obj.id)
        return dics


def df_csv_to_lst(df, cols):
    for col in cols:
        df[col] = df[col].apply(lambda x: x.split(','))


def __id_2_name(dic, id):
    if id != '':
        return dic[id]


def df_cols_id_2_name(df, cols, dic):
    for col in cols:
        df[col] = df[col].apply(lambda x: __id_2_name(dic, x))


def str_to_list(item):
    result = []
    if isinstance(item, str):
        result.append(item)
    elif isinstance(item, list):
        result.extend(item)
    return result