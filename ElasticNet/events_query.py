import pandas as pd
from predicthq import Client
import requests

ACCESS_TOKEN = 'PzDv7ZgjdBecYpF9bR7s2dSASytELd9X3a1N56eR'
phq = Client(access_token=ACCESS_TOKEN)

def get_place_hierarchy(latlong, token):
    # latlong = 'lat,lon'

    response = requests.get(
        url="https://api.predicthq.com/v1/places/hierarchies",
        headers={
          "Authorization": "Bearer " + token,
          "Accept": "application/json"
        },
        params={
            "location.origin": latlong
        }
    )

    return response.json()
def get_place_name(place_id,ACCESS_TOKEN):
    phq = Client(access_token=ACCESS_TOKEN)
    ny_state = phq.places.search(id=place_id).results[0]
    return ny_state.name

place_hierarchy = get_place_hierarchy('47.61091992161532,-122.33669224272333',ACCESS_TOKEN)

place_hierarchy['place_hierarchies'][0][3]['place_id']

get_place_name(4155751,ACCESS_TOKEN)



def pull_events_by_category(categories: list, place_id):
    """
    Pulls events from the events endpoint by category through the python SDK.
    :param lat: float, latitude of the location
    :param long: float, longitude of the location
    :param categories: list, categories to be pulled
    :param radius: int, max radius in kilometers
    :return: phq response
    """
    start = {"gte": consts.params.config("start_gte"), "lte": consts.params.config("start_lte")}
    rank = {"gte": consts.params.config("rank_gte")}
    search_results = consts.phq_client.events.search(
        start=start, state="active", place__scope=place_id, limit=500, category=categories, rank=rank
    )
    return search_results

def create_dataset(results) -> pd.DataFrame:
    all_data = []
    for ev in results.iter_all():
        event_dict = ev.serialize()
        ev_df = pd.DataFrame([event_dict])
        all_data += [ev_df]

    if len(all_data) > 0:
        all_pd = pd.concat(all_data, axis=0)
    else:
        all_pd = pd.DataFrame()
    return all_pd

# def create_dataset(lat, long) -> pd.DataFrame:
#     """
#     Pulls events for one latitude and longitude
#     :param lat: float, latitude of the location
#     :param long: float, longitude of the location
#     :return: pd.DataFrame, events dataframe for the location
#     """
#     all_data = []
#     cat_radius_mapping = (
#         (consts.attended_categories + consts.non_attended_categories, consts.params.config("attended_radius_lte")),
#         (consts.unscheduled_categories, consts.params.config("unscheduled_radius_lte")),
#     )
#     for cat_tup in cat_radius_mapping:
#         results = pull_events_by_category(lat, long, cat_tup[0], cat_tup[1])
#         if results.count > 0 and not results.overflow:
#             for ev in results.iter_all():
#                 event_dict = ev.serialize()
#                 ev_df = pd.DataFrame([event_dict])
#                 all_data += [ev_df]
#         else:
#             assert not results.overflow, "Overflow, reduce scope"
#
#     if len(all_data) > 0:
#         all_pd = pd.concat(all_data, axis=0)
#     else:
#         all_pd = pd.DataFrame()
#     return all_pd


def retrieve_event_data(locations: str, events_path: str) -> None:
    """
    Downloads events for a list of lat longs
    :param locations: File path to the CSV file containing locations, with minimum columns {lat, long, master_id}
    :param events_path: str, path to folder where the events are going to be stored
    :return: None
    """
    loc_df = pd.read_csv(locations)
    assert "lat" in loc_df.columns, "Latitude column missing; It should be named `lat`"
    assert "long" in loc_df.columns, "Longitude column missing; It should be named `long`"
    assert "master_id" in loc_df.columns, "Reference id column missing; It should be named `master_id`"
    print(f"Pulling a total of {loc_df.shape[0]} locations.")
    Path(events_path).mkdir(exist_ok=True)
    for c, row in tqdm(loc_df.iterrows()):
        lat = row["lat"]
        long = row["long"]
        id = row["master_id"]
        id = str(id).replace(".0", "")
        events_df = create_dataset(lat, long)
        if events_df.shape[0] != 0:
            events_df["master_id"] = id
            events_df.to_csv(os.path.join(events_path, f"{id}.csv"), index=False)
        else:
            print(f"No events found for location {id}")
