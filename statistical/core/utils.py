def get_selection_names(indexs, columns, time_parm, filter_dic):
    selections = []
    selections.extend(indexs)
    selections.extend(columns)

    if time_parm and time_parm.segmentation:
        selections.extend(['start_time', 'end_time'])

    if filter_dic:
        selections.extend(filter_dic.keys())

    result = []
    for i in selections:
        si = str(i)
        if result.__contains__(si):
            continue
        result.append(si)
    return result
    # return list(set(selections))  # distinct

def transfer_filter_dic(filter_dic, sports_dic):
    result = {}
    for k, vs in filter_dic.items():
        ids = []
        for v in vs:
            ids.append(sports_dic[v])
        result[k] = ids
    return result