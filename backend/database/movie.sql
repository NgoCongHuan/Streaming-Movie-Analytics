-- Movies
CREATE TABLE movie.movies (
  _id VARCHAR(10) PRIMARY KEY,
  original_title VARCHAR(256),
  english_title VARCHAR(256),
  title VARCHAR(256),
  slug VARCHAR(256),
  overview VARCHAR,
  release_date DATE,
  quality VARCHAR(10),
  rating VARCHAR(10),
  runtime INTEGER,
  type INTEGER,
  status VARCHAR(50),
  latest_season INTEGER,
  year INTEGER,
  imdb_rating DECIMAL,
  video_preview VARCHAR
);

-- Origin Country
CREATE TABLE movie.origin_country (
  _id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(25),
  slug VARCHAR(50)
);

CREATE TABLE movie.movie_origin_countries (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  country_id VARCHAR(10) REFERENCES origin_country(_id)
);

-- Genre
CREATE TABLE movie.genre (
  _id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(50),
  slug VARCHAR(50)
);

CREATE TABLE movie.movie_genres (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  genre_id VARCHAR(10) REFERENCES genre(_id)
);

-- Movie images
CREATE TABLE movie.movie_images (
  image_id SERIAL PRIMARY KEY,
  movie_id VARCHAR(10) REFERENCES movies(_id),
  path VARCHAR,
  type INTEGER,
  category VARCHAR(50)
);

-- Movie latest episodes
CREATE TABLE movie.movie_latest_episodes (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  season_number INTEGER,
  episode_count INTEGER
);

-- Cast
CREATE TABLE movie.casts (
  _id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(256),
  profile_path VARCHAR,
  publish BOOLEAN,
  slug VARCHAR
);

CREATE TABLE movie.movie_casts (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  cast_id VARCHAR(10) REFERENCES casts(_id),
  character VARCHAR,
  cast_order INTEGER,
  created_at BIGINT,
  updated_at BIGINT
);

-- Director
CREATE TABLE movie.director (
  _id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(256),
  slug VARCHAR
);

CREATE TABLE movie.movie_directors (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  director_id VARCHAR(10) REFERENCES director(_id)
);

-- Network
CREATE TABLE movie.network (
  _id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(256),
  slug VARCHAR
);

CREATE TABLE movie.movie_networks (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  network_id VARCHAR(10) REFERENCES network(_id)
);

-- Hãng sản xuất
CREATE TABLE movie.production_company (
  _id VARCHAR(10) PRIMARY KEY,
  name VARCHAR(256),
  slug VARCHAR
);

CREATE TABLE movie.movie_production_companies (
  movie_id VARCHAR(10) REFERENCES movies(_id),
  company_id VARCHAR(10) REFERENCES production_company(_id)
);
