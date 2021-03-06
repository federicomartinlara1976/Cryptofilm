#!/usr/bin/env python

from core import procedimientos as procs
from core import funcionesMongo as fm

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    db = fm.getDatabase()

    procs.insertFilms(db, 'C:/Users/karellrodriguez/Desktop/src/data/movies.json')

    procs.director(db)

    procs.cleanVotes(db)

    procs.genre(db)

    procs.actor(db)

    procs.languages(db)

    procs.countries(db)

    procs.insertUsers(db, 'C:/Users/karellrodriguez/Desktop/src/data/users.json')

    #procs.deleteActors(db)
    #procs.deleteGenres(db)
    #procs.deleteCountries(db)
    #procs.deleteDirectors(db)
    #procs.deleteLanguages(db)
