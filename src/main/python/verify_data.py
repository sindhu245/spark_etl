import pandas as pd

df_edge = pd.read_csv("input/city_table.csv", delimiter =',')
df_edge_state = pd.read_csv("input/state_table.csv", delimiter =',')
df_edge_state_lower = df_edge_state
df_edge_state_lower['short_name'] = df_edge_state_lower['short_name'].str.lower().replace({'ut': 'uk', 'tg': 'ts', 'ct': 'cg', 'or': 'od', 'dd': 'dh'})
df_edge = pd.merge(df_edge, df_edge_state_lower, left_on=['state_id'], right_on=['id'], how='inner')

# df_pulse = pd.read_csv("input/new_pulse_cities.csv")
df_pulse = pd.read_csv('input/test/first.csv')

df_edge['smaller_name'] = df_edge['name_x'].str.lower() #lower case city name

pulse_city_list = df_pulse['city'].values.tolist()

# list of cities which are present in both n left anti in edge data
df_both_cities = df_edge.loc[df_edge['smaller_name'].isin(pulse_city_list)]
both_cities_list = df_both_cities['smaller_name'].values.tolist()

df_both_check_merge = pd.merge(df_edge, df_pulse,
                               left_on=['smaller_name', 'short_name'], right_on=['CITY-NAME', 'REGION'], how='inner')
print("---------")  #names that are same with different state code.
print(len(df_both_check_merge))
print(df_both_check_merge.loc[df_both_check_merge["smaller_name"]=="7th Mile Kalimpong"].head())
df_city_not_in_edge = pd.read_csv('output/new_pulse_cities_not_in_edge.csv')
df_check_missed_cities = df_both_cities.loc[~df_both_cities['smaller_name'].isin(df_both_check_merge['smaller_name'].values.tolist())]
print(len(df_check_missed_cities))
# df_check_missed_cities['name_x'].to_csv('./output/temp.csv', index=False)
print("---------")

df_not_in_pulse_cities = df_edge.loc[~df_edge['smaller_name'].isin(pulse_city_list)]
df_new_pulse_cities = df_pulse[~df_pulse['city'].isin(both_cities_list)]


# print(df_edge_state.head())
print(df_both_cities.head())
print(len(df_both_cities))
print(len(df_new_pulse_cities))
print(len(df_edge))
print(len(df_not_in_pulse_cities))
print(df_not_in_pulse_cities.head())

# df_new_pulse_cities['city_name'].to_csv("./output/new_pulse_cities_not_in_edge.csv", index=False)
# df_not_in_pulse_cities['name_x'].to_csv("./output/edge_cities_not_in_pulse.csv", index=False)
# print(df_both_cities['name'].values.tolist())
# print(df_not_in_pulse_cities['name'].values.tolist())

city_code_change_edge_city_list = ['surathkal', 'jaynagar', 'manipal', 'agucha', 'podanur', 'dharwad u a s', 'kanhangad', 'jagatdal', 'deoprayag', 'cherumulli', 'periashola', 'abarana', 'anpara tps', 'ashokebari', 'badrajote', 'bagdogra', 'bairagir chak', 'balisai', 'gamaharia rampur', 'gariba tola', 'purnea city', 'bhuniajibarh', 'bache r.s.', 'bangursia', 'deepka', 'chaglogam', 'chopelling', 'aideopukhuri', 'jamirapalgarh', 'amlakhi bazar', 'arimora tiniali', 'assam engg. institute', 'atharotilla', 'bakalighat', 'balacherra t e', 'baladhan t e', 'barlongpher', 'barpatra te', 'behara bazar', 'binovanagar', 'borduar', 'bhaba nagar', 'chittaranjan avenue', 'christiankempai', 'chowai', 'dezabra', 'dhanuagaon', 'dhengargaon', 'durah', 'dilkhush t e', 'garsa', 'diphalu satra', 'dumurghat', 'gopinathnagar', 'hajang', 'hayairbond', 'i i t', 'kumbhirgram airport', 'silchar medical college', 'subhang', 'jyotisar', 'abgil chandey', 'mansa devi sector 5', 'agauni', 'sector 20', 'sector-12', 'ajgeba', 'ajokopa', 'akartapa', 'akaunabazar', 'amaon baraon', 'andouli', 'aurahi gobindpur', 'b garhrampur', 'b. dharmapur', 'b. jahidpur', 'b.behta', 'b.fulkaha', 'b.p.ekderwsa', 'babhani bhelwa', 'bakuli', 'balua kachhari', 'alambas', 'barnote', 'bhangarah', 'bhattagarh', 'bheropatti', 'chanderwadi', 'chiliari', 'bhutali malpa', 'd garh itpakwa', 'abburkatte', 'adyanadka', 'esuwa', 'barebettu', 'bhadragola', 'bollaje', 'patna', 'srinagar', 'aligarh', 'haldia', 'nizamabad', 'anantapur', 'bilaspur', 'arunachal', 'dispur', 'badarpur', 'jamalpur', 'nagar', 'samalkha', 'chikhli', 'mansa', 'rajgarh', 'hasanpur', 'pala', 'kushalnagar', 'londa', 'tanda', 'mulki', 'idappadi', 'khurda', 'hamirpur', 'manali', 'nangal', 'kashipur', 'amarda', 'baghuasole', 'aurai', 'madukkarai', 'atchanta', 'c.c.oros', 'badgaon', '7th mile kalimpong', 'anandapur', 'bakla', 'jehanabad r.s.', 'bharatpur', 'nagari', 'dharampur', 'islampur', 'kusumba', 'baroda', 'begunia', 'baradwar', 'deorali', 'chimbhave', 'kadamtala', 'gamharia', 'muradpur', 'gandhinagar', 'amoda', 'baghdoba', 'rampur', 'anjan', 'badia', 'tarapur', 'sultanpur', 'banka', 'rajnagar', 'industrial estate', 'amjhar', 'ambari', 'amgaon', 'ambabar', 'haripur', 'silcuri', 'kurukshera universsity kuruksh', 'baghmari', 'amolwa', 'amrainawada', 'bargaon', 'asarhi', 'b. bhelahi', 'baksoti', 'airwan', 'bans ghat', 'chandryan dharhara', 'chaturbhujibaraon', 'akchamal', 'alchi', 'bargo', 'baroo', 'basantgarh', 'bhawani', 'basantpur', 'bhimbat', 'bodhkharboo', 'chamray', 'chuchot gongma', 'chuglamsar', 'diskit', 'karsha', 'khasgam thuina', 'damgarhi', 'khandel', 'yamunanagar', 'kargil', 'leh city', 'pulwama', 'udhampur', 'rudarpur', 'ramagundam', 'kachana', 'nagaram', 'ghatkesar', 'kondagaon', 'khandagiri', 'rajgangpur', 'madhapur', 'manuguru']
df_city_code_change_edge_city_list = df_both_cities.loc[df_both_cities['smaller_name'].isin(city_code_change_edge_city_list)]
# print(len(df_city_code_change_edge_city_list))
# print(df_city_code_change_edge_city_list['name'].values.tolist())

#checking if new pulse cities and the second list shared by akhil are same or not.
df_pulse_read = pd.read_csv("input/new_pulse_cities.csv")
# print('---------------\n', len(df_pulse_read))
df_pulse_city_code_state_name = pd.read_csv("input/test_pulse_city_code.csv")
# print('---------------\n', len(df_pulse_city_code_state_name))

df_check_pulse_selected_cities = pd.merge(df_pulse_read, df_pulse_city_code_state_name,
                                          left_on=['city'], right_on=['CITY-NAME'], how='inner')
# print(len(df_check_pulse_selected_cities))
# df_check_pulse_selected_cities.to_csv("check_all_column_coming.csv")