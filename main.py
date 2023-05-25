import pandas as pd
import numpy as np
from geopy.geocoders import Nominatim
from geopy.distance import geodesic
from sqlalchemy import create_engine

class DataProcessing:
    def __init__(self, csv_file):
        self.df = pd.read_csv(csv_file)
        
    def get_unique_places(self):
        origin = self.df['origin']
        destination = self.df['destination']
        place = origin.append(destination)
        place = place.drop_duplicates()
        place = place.reset_index()
        place = place.drop(['index'], axis=1)
        place['address'] = 0
        place['latitude'] = 0
        place['longitude'] = 0
        return place
        
    def get_location_info(self, place):
        # Initialize Nominatim API
        geolocator = Nominatim(user_agent="abin.devassia@letstransport.team")
        for i in range(0, len(place)):
            try:
                hub = place.iloc[i, 0]
                location = geolocator.geocode(hub, timeout=1000000)
                place.loc[i, 'address'] = location.address
                place.loc[i, 'latitude'] = location.latitude
                place.loc[i, 'longitude'] = location.longitude
            except (ValueError, AttributeError):
                pass
        return place
    
    def process_bookings(self, column):
        bookings = self.df.drop([column, 'lane', 'year_month'], axis=1)
        print(bookings.head())
        print(bookings.columns[0])
        bookings = pd.DataFrame(bookings.groupby(bookings.columns[0]).agg({bookings.columns[1]:'sum'}).reset_index())
        bookings.columns = ['place','bookings']
        return bookings    
    def place_bookings_positions(self,total_bookings,place):
        result = pd.concat([total_bookings, place[['latitude', 'longitude']]], axis=1, join='inner')
        result = result.iloc[:,[0,2,3,1]]
        print(result.head())
        return result
    def distance_bw_places(self,tot_bookings_with_positions):
        
        rows = len(df)+1
        cols = len(df)+1
        mat = [[0 for _ in range(cols)] for _ in range(rows)]
        m = 0

        # naming column heading as places * places matrix. With cells as the distance between each places. 

        for m in range(0,len(df)):
            mat[0][m+1] = df.loc[m,'place']
            m = m + 1
        m = 0
        for m in range(0,len(df)):
            mat[m+1][0] = df.loc[m,'place']
            m = m + 1
        n = 0
        for n in range(0, len(df)):
            m = 0
            for m in range(0, len(df)):
                coords_1 = (df.loc[n,'latitude'],df.loc[n,'longitude'])
                coords_2 = (df.loc[m,'latitude'],df.loc[m,'longitude'])
                mat[m+1][n+1] = geodesic(coords_1, coords_2).km
                m = m+1
            n = n+1
        return mat
    def clustering_places_less_than_k_km(self,mat):
        rows = len(mat)
        cols = len(mat)
        mat_0 = [[0 for _ in range(cols)] for _ in range(rows)]
        n = 0
        while(n<len(mat[0])-1):
            A = []
            m = 0
            while(m<len(mat)-1):
                a = float(mat[m+1][n+1])
                mat_0[0][n] = mat[0][n+1]
                if(0 < a <= 200.0):
                    A.append(mat[m+1][0])
                    #m = m+1
                    mat_0[m+1][n] = mat[m+1][0]
                    mat = np.delete(mat, m+1, 0)
                    mat = np.delete(mat, m+1, 1)
                    #m = m-1
                else:
                    m = m+1
            n = n+1

        mat_0 = pd.DataFrame(mat_0)
        print(mat_0)
        mat_0 = mat_0.loc[(mat_0!=0).any(axis=1)]
        mat_0 = mat_0.loc[:, (mat_0!= 0).any(axis=0)]
        mat_0 = mat_0.reset_index()
        mat_0 = mat_0.drop(['index'], axis = 1)

        mat_1 = mat_0.copy()
        i = 0
        cluster = pd.DataFrame(np.zeros((len(mat_1), len(mat_0.columns))))
        for i in range(0,len(cluster.columns)):
            df = mat_1.iloc[:,i]
            df = pd.DataFrame(df)
            for j in range(0,len(df)):
                if(df.iloc[j,0] == 0):
                    df.iloc[j,0] = '0'
                    j = j+1
                else: j = j+1
        df = df.sort_values(i,ascending=False)
        df = df.reset_index()
        df = df.drop(['index'], axis = 1)
        cluster.iloc[:,i] = df

        A = []
        l = 0
        for l in range(0, len(cluster.columns)):
            df = cluster.iloc[:,l]
            df1 = pd.DataFrame(np.zeros((len(df), 2)))

            i = 0
            for i in range(0, len(df) - 1):
                if(df[i] == '0'):
                    break
                else: 
                    j = 0
                    for j in range(0, len(dfm)):
                        if(df[i] == dfm.iloc[j,0]):
                            df1.iloc[i,0] = dfm.iloc[j,0]
                            df1.iloc[i,1] = dfm.iloc[j,3]
                            j = j+1
                            i = i+1
                        else:
                            j = j+1        

            maxi = max(df1.iloc[:,1])
            k = 0
            for k in range(0, len(df1)):
                if(maxi == df1.iloc[k,1]):
                    A.append(df1.iloc[k,0])
                    break
                else: k = k+1

            l = l+1

        cluster.columns = A
        return cluster
    def final_cluster(self, cluster):
        df = cluster.iloc[:,0]
        pd.DataFrame(df)
        df = cluster.iloc[:,0]
        df = pd.DataFrame(df)
        df = df.loc[(df!= '0').any(axis=1)]
        df = df.reset_index()
        df = df.drop(['index'], axis = 1)
        df['cluster'] = cluster.columns[0]+' cluster'
        
        df = cluster.iloc[:,0]
        df = pd.DataFrame(df)
        df = df.loc[(df!= '0').any(axis=1)]
        df = df.reset_index()
        df = df.drop(['index'], axis = 1)
        df['cluster'] = cluster.columns[0]+' cluster'
        df.rename(columns = {cluster.columns[0]:'Place'}, inplace = True)
        df

        i = 1
        for i in range(1, len(cluster.columns) - 1):
            df1 = cluster.iloc[:,i]
            df1 = df1.fillna(0)
            df1 = pd.DataFrame(df1)
            df1 = df1.loc[(df1!= '0').any(axis=1)]
            df1 = df1.reset_index()
            df1 = df1.drop(['index'], axis = 1)
            df1['cluster'] = cluster.columns[i]+' cluster'
            df1.rename(columns = {cluster.columns[i]:'Place'}, inplace = True)
            df = df.append(df1)
        
if __name__ == "__main__":
    # Define input parameters
    csv_file = 'data.csv'
    data = DataProcessing(csv_file)
    data.df = data.df.drop(columns='Unnamed: 0')


    # Instantiate DataProcessing class and execute methods
    # place = data.get_unique_places()
    # place = data.get_location_info(place)
    
    # Process origin bookings
    bookings_destination = data.process_bookings('origin')
    print("Origin Bookings:")
    print(bookings_origin)

    # Process destination bookings
    bookings_origin = data.process_bookings('destination')
    print("Destination Bookings:")
    print(bookings_destination)

    # Combine origin and destination bookings
    bookings_combined = bookings_origin.append(bookings_destination)
    bookings_combined = bookings_combined.reset_index(drop=True)
    print("Combined Bookings:")
    print(bookings_combined)
    total_bookings =  bookings_combined.groupby('place').agg({'bookings':'sum'}).reset_index()
    print(total_bookings.shape)
    tot_bookings_with_positions=data.place_bookings_positions(total_bookings,place)
    mat = distance_bw_places(tot_bookings_with_positions)
    cluster = clustering_places_less_than_k_km(mat)
    df=final_cluster(cluster)

    
