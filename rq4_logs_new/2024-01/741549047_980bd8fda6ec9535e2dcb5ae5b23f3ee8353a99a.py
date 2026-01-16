import streamlit as st
import json


st.title('Habit Tracker')


# habits = ['Exercise', 'Eat Healthy', 'Meditate', 'Read', 'Sleep Early', 'Wake Up Early']

# Load data
#@st.cache_data()
def load_data():
    with open('data.json', 'r') as f:
        data = json.load(f)
    return data

data = load_data()
habits = data['habits']
frequency_options = data['frequency_options']



# create new habit in sidebar
def create_new_habit():
    st.sidebar.title('Create New Habit')
    new_habit_name = st.sidebar.text_input('Habit Name:')

    add_button = st.sidebar.button('Add habit')
    if add_button:
        new_habit = {
            'name': new_habit_name,
            'frequency_goal': 'Daily',
            'goal_total_times_input': 0,
            'done_times': 0
        }
        if new_habit_name != '' and new_habit_name not in [habit['name'] for habit in habits]:
            habits.append(new_habit)
            with open('data.json', 'w') as f:
                json.dump(data, f, indent=4)


# delete habit in sidebar
def delete_new_habit():
    st.sidebar.title('Delete Habit')
    habit_names = [''] + [habit['name'] for habit in habits]
    delete_habit_name = st.sidebar.selectbox('Habit Name:', habit_names)
    delete_button = st.sidebar.button('Delete habit')
    if delete_button and delete_habit_name != '':
        for habit in habits:
            if habit['name'] == delete_habit_name:
                habits.remove(habit)
                with open('data.json', 'w') as f:
                    json.dump(data, f, indent=4)
                break


create_new_habit()
delete_new_habit()

# Overall progress 
st.markdown('**Overall Progress**')
total_done_times = sum([habit['done_times'] for habit in habits])
total_goal_times = sum([habit['goal_total_times_input'] * frequency_options[habit['frequency_goal']] for habit in habits])

try:
    text = f'{total_done_times}/{total_goal_times}   ~  {total_done_times/total_goal_times*100:.0f}%' 
    if total_done_times > total_goal_times:
        progress_bar = st.progress(1.0, text=text)
    else:
        progress_bar = st.progress(total_done_times / total_goal_times, text=text)
except:
    progress_bar = st.progress(0, text='0/0')

    


# Create columns
columns = st.columns(len(habits))


for i in range(len(habits)):
    with columns[i]:
        st.markdown(f'**{habits[i]["name"]}**')
        goal_total_times_input = st.number_input('Times Goal:', min_value=0, max_value=30, step=1, key=f'goal{i}')
        # Custom every time period: Year
        predefined_frequency_index = list(frequency_options.keys()).index(habits[i]['frequency_goal'])
        frequency_goal = st.selectbox('Every:', frequency_options.keys(), key=f'frequency_goal{i}', index=predefined_frequency_index)
        # Done number times
        done_times = st.number_input('Times Done:', min_value=0, max_value=9999, step=1, key=f'done{i}')

        goal_total_times = goal_total_times_input * frequency_options[frequency_goal]
        

        # Progress bar
        try:
            text = f'{done_times}/{goal_total_times}   ~  {done_times/goal_total_times*100:.0f}%' 
            if done_times > goal_total_times:
                progress_bar = st.progress(1.0, text=text)
            else:
                progress_bar = st.progress(done_times / goal_total_times, text=text)
        except:
            progress_bar = st.progress(0, text='0/0')

        # Save data
        habits[i]['goal_total_times_input'] = goal_total_times_input
        habits[i]['frequency_goal'] = frequency_goal
        habits[i]['done_times'] = done_times

        # Save data to json file
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)