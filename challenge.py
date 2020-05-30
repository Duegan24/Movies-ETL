def movie_data_cleaner_to_postgres(wiki_data_path, kaggle_metadata_path, kaggle_rating_data_path):
    import re, sys, time, json
    import pandas as pd
    import numpy as np
    from sqlalchemy import create_engine, delete
    from  config import db_password


    def clean_movie(movie):
    
        movie = dict(movie) #create a non-destructive copy
        alt_titles = {}
        
        # combine alternate titles into one list
        for key in ['Also known as','Arabic','Cantonese','Chinese','French',
                    'Hangul','Hebrew','Hepburn','Japanese','Literally',
                    'Mandarin','McCune–Reischauer','Original title','Polish',
                    'Revised Romanization','Romanized','Russian',
                    'Simplified','Traditional','Yiddish']:
            if key in movie:
                alt_titles[key] = movie[key]
                movie.pop(key)
        if len(alt_titles) > 0:
            movie['alt_titles'] = alt_titles
        
        # merge column names
        def change_column_name(old_name, new_name):
            if old_name in movie:
                movie[new_name] = movie.pop(old_name)
        
        change_column_name('Adaptation by', 'Writer(s)')
        change_column_name('Country of origin', 'Country')
        change_column_name('Directed by', 'Director')
        change_column_name('Distributed by', 'Distributor')
        change_column_name('Edited by', 'Editor(s)')
        change_column_name('Length', 'Running time')
        change_column_name('Original release', 'Release date')
        change_column_name('Music by', 'Composer(s)')
        change_column_name('Produced by', 'Producer(s)')
        change_column_name('Producer', 'Producer(s)')
        change_column_name('Productioncompanies ', 'Production company(s)')
        change_column_name('Productioncompany ', 'Production company(s)')
        change_column_name('Released', 'Release Date')
        change_column_name('Release Date', 'Release date')
        change_column_name('Screen story by', 'Writer(s)')
        change_column_name('Screenplay by', 'Writer(s)')
        change_column_name('Story by', 'Writer(s)')
        change_column_name('Theme music composer', 'Composer(s)')
        change_column_name('Written by', 'Writer(s)')    
                
        return movie

    def parse_dollars(s):
        # if s is not a string, return NaN
        if type(s) != str:
            return np.nan

        # if input is of the form $###.# million
        if re.match(r'\$\s*\d+\.?\d*\s*milli?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " million"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a million
            value = float(s) * 10**6

            # return value
            return value

        # if input is of the form $###.# billion
        elif re.match(r'\$\s*\d+\.?\d*\s*billi?on', s, flags=re.IGNORECASE):

            # remove dollar sign and " billion"
            s = re.sub(r'\$|\s|[a-zA-Z]','', s)

            # convert to float and multiply by a billion
            value = float(s) * 10**9

            # return value
            return value

        # if input is of the form $###,###,###
        elif re.match(r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)', s, flags=re.IGNORECASE):

            # remove dollar sign and commas
            s = re.sub(r'\$|,','', s)

            # convert to float
            value = float(s)

            # return value
            return value
            
        # otherwise, return NaN
        else:
            return np.nan
    


    try:
        # Load Wiki Data - Assumption is that the Wiki file is a JSON formated file
        with open(wiki_data_path, mode='r') as file:
            wiki_movies_raw = json.load(file)
        # Load Kaggle Metadata  - Assumption is that the Kaggle Metadata file is a csv file
        kaggle_metadata = pd.read_csv(kaggle_metadata_path, low_memory=False)
        # Load Kaggle Rating Data  - Assumption is that the ratings data is a csv file
        ratings = pd.read_csv(kaggle_rating_data_path)
    except:
        e = sys.exc_info()[0]
        print( "Error while loading data: %s" % e )

    try:
        # Clean Wiki Movies - Primary assumuptions here are that the column headings are identical - header cleaning assumes specific names
        ## Only include those records that have a director, imdb_link and does not include number of episodes
        wiki_movies = [movie for movie in wiki_movies_raw
                if ('Director' in movie or 'Directed by' in movie)
                    and 'imdb_link' in movie
                    and 'No. of episodes' not in movie]
        ## Clean up the redundant columns
        clean_movies = [clean_movie(movie) for movie in wiki_movies]
        ## Load the Wiki movies data as a dataframe
        wiki_movies_df = pd.DataFrame(clean_movies)
        ## Remove Duplicates from Clean Movies Dataframe
        wiki_movies_df['imdb_id'] = wiki_movies_df['imdb_link'].str.extract(r'(tt\d{7})')
        wiki_movies_df.drop_duplicates(subset='imdb_id', inplace=True)
        ## Remove columns that have values in less than 10% of the records - Assumption here is that the 10% that are removed are not useful
        clean_columns_to_keep = [column for column in wiki_movies_df.columns if wiki_movies_df[column].isnull().sum() < len(wiki_movies_df)*0.9]
        wiki_movies_df = wiki_movies_df[clean_columns_to_keep]

        # Box Office values clean up
        box_office = wiki_movies_df['Box office'].dropna()
        box_office = box_office.apply(lambda x: ' '.join(x) if type(x) == list else x)
        ## Define the regular expressions that we will use to clean up the box office revenue - Assumptions here is that a very small fraction have forms that are not covered here.
        form_one = r'\$\s*\d+\.?\d*\s*[mb]illi?on'
        form_two = r'\$\s*\d{1,3}(?:[,\.]\d{3})+(?!\s[mb]illion)'
        ## Replace ranges of values with one value
        box_office = box_office.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        ## Regex applied to only keep the revenue
        wiki_movies_df['box_office'] = box_office.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        ## Remove old column that was replaced
        wiki_movies_df.drop('Box office', axis=1, inplace=True)

        #  Clean budget data
        budget = wiki_movies_df['Budget'].dropna()
        ## Do some basic clean up and convert lists to string
        budget = budget.map(lambda x: ' '.join(x) if type(x) == list else x)
        ## Remove ranges of values
        budget = budget.str.replace(r'\$.*[-—–](?![a-z])', '$', regex=True)
        ## Remove numbers with brackets...references
        budget = budget.str.replace(r'\[\d+\]\s*', '')
        ## use forms from box office to keep only the revenue results
        wiki_movies_df['budget'] = budget.str.extract(f'({form_one}|{form_two})', flags=re.IGNORECASE)[0].apply(parse_dollars)
        ## Drop old column
        wiki_movies_df.drop('Budget', axis=1, inplace=True)

        # Clean Up Release Date
        release_date = wiki_movies_df['Release date'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        ## Define the regular expressions that we will use to clean up the release dates
        ### Month Name two digit or one digit day comma and four digit year - Assumptions here is that a very small fraction have forms that are not covered here.
        date_form_one = r'(\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|(Nov|Dec)(?:ember)?)\D?(\d{1,2}\D?)?\D?([12]\d{3}))'
        date_form_two = r'(([12]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])))'
        date_form_three = r'(\b(Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:tember)?|Oct(?:ober)?|(Nov|Dec)(?:ember)?)\s*([12]\d{3}))'
        date_form_four = r'\d{4}'
        ## Apply forms to clean the date information
        wiki_movies_df['release_date'] = pd.to_datetime(release_date.str.extract(f'({date_form_one}|{date_form_two}|{date_form_three}|{date_form_four})')[0], infer_datetime_format=True)
        ## Drop old column
        wiki_movies_df.drop('Release date', axis=1, inplace=True)

        # Clean Running Time
        running_time = wiki_movies_df['Running time'].dropna().apply(lambda x: ' '.join(x) if type(x) == list else x)
        ## Created a datatable of the hour + minutes or just minutes. Each round bracket creates a new column in the datatable - Assumptions here is that a very small fraction have forms that are not covered here.
        running_time_extract = running_time.str.extract(r'(\d+)\s*ho?u?r?s?\s*(\d*)|(\d+)\s*m')
        ## Fill all NaN values with zero...the to_numeric is converting the strings to numeric values if it is not then it will throw an error
        ## which is then replaced with a Na and the fillna is chained in to replace the na with a zero
        running_time_extract = running_time_extract.apply(lambda col: pd.to_numeric(col, errors='coerce')).fillna(0)
        # Calculate the running time into minutes
        wiki_movies_df['running_time'] = running_time_extract.apply(lambda row: row[0]*60 + row[1] if row[2] == 0 else row[2], axis=1)
        ## Drop old column
        wiki_movies_df.drop('Running time', axis=1, inplace=True)
    except:
        e = sys.exc_info()[0]
        print( "Error Cleaning Wiki Data: %s" % e )

    try:
        # Kaggle Data Cleanup - Assumption is that all the column names do not change
        kaggle_metadata = kaggle_metadata[kaggle_metadata['adult'] == 'False'].drop('adult',axis='columns')
        ## Column Cleaning to correct data types
        kaggle_metadata['video'] = kaggle_metadata['video'] == 'True'
        kaggle_metadata['budget'] = kaggle_metadata['budget'].astype(int)
        kaggle_metadata['id'] = pd.to_numeric(kaggle_metadata['id'], errors='raise')
        kaggle_metadata['popularity'] = pd.to_numeric(kaggle_metadata['popularity'], errors='raise')
        kaggle_metadata['release_date'] = pd.to_datetime(kaggle_metadata['release_date'])
    except:
        e = sys.exc_info()[0]
        print( "Error while cleaning Kaggle Metadata: %s" % e )

    try:
        # Merge movie data into one dataframe and clean up redundancies
        movies_df = pd.merge(wiki_movies_df, kaggle_metadata, on='imdb_id', suffixes=['_wiki','_kaggle'])
        ## Remove redundant columns
        movies_df.drop(columns=['title_wiki','release_date_wiki','Language','Production company(s)'], inplace=True)
        ## Create a function to quickly fill in missing data in the kaggle columns with data from the wiki columns
        def fill_missing_kaggle_data(df, kaggle_column, wiki_column):
            df[kaggle_column] = df.apply(
                lambda row: row[wiki_column] if row[kaggle_column] == 0 else row[kaggle_column]
                , axis=1)
            df.drop(columns=wiki_column, inplace=True)
        ## Fill in the missing data
        fill_missing_kaggle_data(movies_df, 'runtime', 'running_time')
        fill_missing_kaggle_data(movies_df, 'budget_kaggle', 'budget_wiki')
        fill_missing_kaggle_data(movies_df, 'revenue', 'box_office')
        ## Drop the video column that doesn't have any useful information
        movies_df.drop('video',axis=1,inplace=True)

        # Final Restructuring/renaming of columns
        ## Reorder the Columns to be more useful  - Assumption is that all the column names do not change
        movies_df = movies_df.loc[:, ['imdb_id','id','title_kaggle','original_title','tagline','belongs_to_collection','url','imdb_link',
                            'runtime','budget_kaggle','revenue','release_date_kaggle','popularity','vote_average','vote_count',
                            'genres','original_language','overview','spoken_languages','Country',
                            'production_companies','production_countries','Distributor',
                            'Producer(s)','Director','Starring','Cinematography','Editor(s)','Writer(s)','Composer(s)','Based on'
                            ]]
        ## Rename the columns to be more descriptive
        movies_df.rename({'id':'kaggle_id',
                    'title_kaggle':'title',
                    'url':'wikipedia_url',
                    'budget_kaggle':'budget',
                    'release_date_kaggle':'release_date',
                    'Country':'country',
                    'Distributor':'distributor',
                    'Producer(s)':'producers',
                    'Director':'director',
                    'Starring':'starring',
                    'Cinematography':'cinematography',
                    'Editor(s)':'editors',
                    'Writer(s)':'writers',
                    'Composer(s)':'composers',
                    'Based on':'based_on'
                    }, axis='columns', inplace=True)
    except:
        e = sys.exc_info()[0]
        print( "Error while merging into Movies Dataframe: %s" % e )

    try:
        # Rating data cleanup - Assumption is that all the column names do not change
        ratings['timestamp'] = pd.to_datetime(ratings['timestamp'], unit='s')
        ## Group the data by movieID and rating, rename teh userID to count...since that is what it is now,
        ## and pivot so the index is now movie ID
        rating_counts = ratings.groupby(['movieId','rating'], as_index=False).count() \
                        .rename({'userId':'count'}, axis=1) \
                        .pivot(index='movieId',columns='rating', values='count')
        rating_counts.columns = ['rating_' + str(col) for col in rating_counts.columns]

        # Perform a Left Join merge with movies and ratings data
        movies_with_ratings_df = pd.merge(movies_df, rating_counts, left_on='kaggle_id', right_index=True, how='left')
        movies_with_ratings_df[rating_counts.columns] = movies_with_ratings_df[rating_counts.columns].fillna(0)
    except:
        e = sys.exc_info()[0]
        print( "Error while cleaning and merging Ratings data: %s" % e )

    try:
        #  Connect to postgres server  - Assumptin is that the database file is movie_data
        connection_string = f"postgres://postgres:{db_password}@localhost:5432/movie_data"
        engine = create_engine(connection_string)

        # Delete any records in movies table
        engine.delete("movies")

        # Push Movies DataFrame to postgres database
        movies_df.to_sql(name='movies', if_exists='append', con=engine)
    except:
        e = sys.exc_info()[0]
        print("Error while sending Movies DataFram to postgres: %s" % e )

    try:
        # Import Ratings Data CSV file into the database
        rows_imported = 0
        start_time = time.time()
        for data in pd.read_csv(kaggle_rating_data_path, chunksize=1000000):
            print(f'importing rows {rows_imported} to {rows_imported + len(data)}...', end='')
            data.to_sql(name='ratings', con=engine, if_exists='replace')
            rows_imported += len(data)
            # add elapsed time to final print out
            print(f'Done. {time.time() - start_time} total seconds elapsed')
    except:
        e = sys.exc_info()[0]
        print("Error while sending Movies DataFram to postgres: %s" % e )
    
    return





# Testing Script to make sure the function worked as required.

file_dir = 'C:/Users/jjgla/Documents/GitHub/Movies-ETL/'
wiki_data_path = f'{file_dir}wikipedia.movies.json'
kaggle_metadata_path = f'{file_dir}movies_metadata.csv'
kaggle_rating_data_path = f'{file_dir}ratings.csv'

movie_data_cleaner_to_postgres(wiki_data_path, kaggle_metadata_path, kaggle_rating_data_path)

