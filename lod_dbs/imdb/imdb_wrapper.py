from imdb import IMDb

class IMDBWrapper:
    def __init__(self):
        self.__ia = IMDb()

    def get_film_by_id(self, film_id):
        """
        Takes an imdb film id and returns a dictionary with information
        about this film.

        Keys are (all optional):
            title - the title of the movie,
            initial_release_date - a year or ???? if unknown,
            directed_by - a list of names
            written_by - a list of names
            produced_by - a list of names
            actors - a list of imdb actor ids
            genre - the genre of the movie
            plot - a list of plots for that movie
        """
        result = {}
        movie = self.__ia.get_movie(film_id)
        if movie:
            if movie.has_key('title'):
                result['name'] = movie['title']
            if movie.has_key('year'):
                result['initial_release_date'] = movie['year']
            if movie.has_key('director'):
                result['directed_by'] = [a['name'] for a in movie['director']]
            if movie.has_key('writer'):
                result['written_by'] = [a['name'] for a in movie['writer']]
            if movie.has_key('producer'):
                result['produced_by'] = [a['name'] for a in movie['producer']]
            if movie.has_key('genre'):
                result['genre'] = movie['genre']
            if movie.has_key('actors'):
                result['actors'] = [a.getID() for a in movie['cast']]
            if movie.has_key('plot'):
                result['description'] = [plot.split('::')[0] for plot in
                        movie['plot']]
        return result
