from statistical.utils.linq import Linq


def get_id_by_name(dic_list, name):
    item = Linq(dic_list).first_or_none(lambda x: x.name == name)
    if item:
        return item.id


def get_name_by_id(dic_list, id):
    item = Linq(dic_list).first_or_none(lambda x: int(x.id) == int(id))
    if item:
        return item.name
