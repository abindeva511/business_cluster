import pandas as pd
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

    
