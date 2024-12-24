import pandas as pd

# state check
df_edge_state = pd.read_csv("input/state_table.csv", delimiter =',')
df_edge_state["state_smaller_name"] = df_edge_state['name'].str.lower()

df_pulse_state = pd.read_csv("input/state_code_pulse.csv")
df_pulse_state = df_pulse_state.loc[df_pulse_state['COUNTRY'] == 'ind']
df_pulse_state = df_pulse_state.rename(columns={"REGION-DESC": "state_smaller_name"})

df_outer_state = pd.merge(df_edge_state, df_pulse_state, on='state_smaller_name', how='outer')
df_outer_state['check_code'] = df_outer_state['netacuity_id'] == df_outer_state['REGION-CODE']

# & df_pulse_state['REGION'] != '?'

# print(df_pulse_state.head())
# print(df_edge_state.head())
# print(len(df_pulse_state))
# print(df_outer_state.head())
# print(len(df_outer_state))
# df_outer_state.to_csv("./output/state_code_list.csv")


# --------------------------city code check-----------------------------#
df_edge_city = pd.read_csv("input/city_table.csv", delimiter =',')
df_edge_city["city_smaller_name"] = df_edge_city['name'].str.lower()

df_edge_state_lower = df_edge_state
df_edge_state_lower['short_name'] = df_edge_state_lower['short_name'].str.lower().replace({'ut': 'uk', 'tg': 'ts', 'ct': 'cg', 'or': 'od', 'dd': 'dh'})
df_edge_city = pd.merge(df_edge_city, df_edge_state_lower, left_on=['state_id'], right_on=['id'], how='inner')
print(df_edge_city.head())
# df_edge_city.to_csv("temp.csv")

df_pulse_city = pd.read_csv("input/test_pulse_city_code.csv")
# df_pulse_city = pd.read_csv("input/city_code_pulse.csv")
# df_pulse_city = df_pulse_city.loc[df_pulse_city['COUNTRY'] == 'ind']
print(df_pulse_city.shape)
df_pulse_city = df_pulse_city.rename(columns={"CITY-NAME": "city_smaller_name"})

df_outer_city = pd.merge(df_edge_city, df_pulse_city, left_on=['city_smaller_name', 'short_name'], right_on=['city_smaller_name', 'REGION'], how='left')

# print(df_outer_city.columns)
df_outer_city['check_code'] = (df_outer_city['netacuity_id_x'] == df_outer_city['CITY-CODE']) & (df_outer_city['short_name'] == df_outer_city['REGION'])

# print(len(df_edge_city))
print(len(df_pulse_city))
print(len(df_outer_city))
df_outer_city_temp = df_outer_city[['city_smaller_name', 'netacuity_id_x', 'CITY-CODE', 'short_name', 'REGION', 'check_code']]
df_outer_city_temp.to_csv("./output/test_city_code_list.csv")

# check the following cities are present in pulse city list without consideration of state.
empty_city_code_in_pulse = ['chaglogam', 'chopelling', 'aideopukhuri', 'amlakhi bazar', 'arimora tiniali', 'assam engg. institute', 'atharotilla', 'baghdoba', 'bakalighat', 'balacherra t e', 'baladhan t e', 'barlongpher', 'barpatra te', 'behara bazar', 'binovanagar', 'borduar', 'chittaranjan avenue', 'christiankempai', 'dezabra', 'dhanuagaon', 'dhengargaon', 'dilkhush t e', 'diphalu satra', 'dumurghat', 'gopinathnagar', 'hajang', 'hayairbond', 'i i t', 'kumbhirgram airport', 'silchar medical college', 'subhang', 'jaynagar', 'gamaharia rampur', 'gariba tola', 'purnea city', 'abgil chandey', 'agauni', 'ajgeba', 'ajokopa', 'akartapa', 'akaunabazar', 'amaon baraon', 'andouli', 'aurahi gobindpur', 'b garhrampur', 'b. dharmapur', 'b. jahidpur', 'b.behta', 'b.fulkaha', 'b.p.ekderwsa', 'babhani bhelwa', 'bakuli', 'balua kachhari', 'bargo', 'bhangarah', 'bhattagarh', 'bheropatti', 'bhutali malpa', 'd garh itpakwa', 'damgarhi', 'esuwa', 'bache r.s.', 'bangursia', 'baradwar', 'chimbhave', 'deepka', 'bhaba nagar', 'chowai', 'durah', 'garsa', 'jyotisar', 'mansa devi sector 5', 'sector 20', 'sector-12', 'akchamal', 'alambas', 'alchi', 'barnote', 'baroo', 'basantgarh', 'bhimbat', 'bodhkharboo', 'chamray', 'chanderwadi', 'chiliari', 'chuchot gongma', 'chuglamsar', 'diskit', 'karsha', 'khasgam thuina', 'kargil', 'leh city', 'surathkal', 'londa', 'manipal', 'dharwad u a s', 'abburkatte', 'adyanadka', 'barebettu', 'bhadragola', 'bollaje', 'kanhangad', 'khandel', 'khurda', 'agucha', 'podanur', 'cherumulli', 'periashola', 'deoprayag', 'abarana', 'anpara tps', 'jagatdal', 'ashokebari', 'badrajote', 'bagdogra', 'bairagir chak', 'balisai', 'bhuniajibarh', 'jamirapalgarh']
df_empty_city_code_in_pulse = df_pulse_city.loc[df_pulse_city['city_smaller_name'].isin(empty_city_code_in_pulse)]

# print(len(df_empty_city_code_in_pulse))
# print(df_empty_city_code_in_pulse.head(100))



