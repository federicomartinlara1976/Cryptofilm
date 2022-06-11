import vars as v


def generateLike(view):
    # Genero el like a partir de la vista
    like = {}
    like[v.userString] = view[v.userString]
    like[v.titleString] = view[v.titleString]
    like[v.yearString] = view[v.yearString]

    return like
