import streamlit as st
import preprocessor, helper
import matplotlib.pyplot as plt
import seaborn as sns

st.sidebar.title("Whatsapp Chat Analyzer")

uploaded_file = st.sidebar.file_uploader("Choose a file")
if uploaded_file is not None:
    bytes_data = uploaded_file.getvalue()
    data = bytes_data.decode("utf-8")
    df = preprocessor.preprocess(data)

    # Fetch unique users
    user_list = df['user'].unique().tolist()
    user_list.remove('group_notification')
    user_list.sort()
    user_list.insert(0, "Overall")

    selected_user = st.sidebar.selectbox("Show analysis wrt", user_list)

    if st.sidebar.button("Show Analysis"):
        # Ensure that fetch_stats function returns the expected values
        try:
            num_messages, words, num_media_messages, num_links = helper.fetch_stats(selected_user, df)
        except Exception as e:
            st.error(f"Error fetching stats: {e}")
            num_messages = words = num_media_messages = num_links = 0  # Default values

        # Inject custom CSS
        st.markdown("""
            <style>
            .title {
                font-family: 'Arial', sans-serif;
                font-size: 36px;  /* Adjust font size */
                font-weight: bold;
                color: white;     /* Set text color to white */
                text-align: center;
                margin-top: 20px;
            }
            body {
                background-color: #333;  /* Dark background to make the white text stand out */
                color: white;            /* Set default text color to white for consistency */
            }
            .plot {
                padding: 20px;          /* Add padding around plots */
                background-color: #444; /* Darker background for plots */
                border-radius: 8px;     /* Rounded corners for plots */
                margin-bottom: 40px;    /* Add space between plots */
            }
            .emoji-table {
                width: 100%;             /* Full width */
                max-width: 800px;        /* Max width */
                overflow-x: auto;        /* Horizontal scroll if necessary */
                padding: 20px;          /* Padding around table */
                background-color: #444; /* Darker background for table */
                border-radius: 8px;     /* Rounded corners for table */
            }
            </style>
            <div class="title">Top Statistics</div>
        """, unsafe_allow_html=True)

        # Displaying statistics
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            st.header("Total Messages")
            st.subheader(num_messages)
        with col2:
            st.header("Total Words")
            st.subheader(words)
        with col3:
            st.header("Media Shared")
            st.subheader(num_media_messages)
        with col4:
            st.header("Links Shared")
            st.subheader(num_links)

        # Monthly Timeline
        st.title("Monthly Timeline")
        timeline = helper.monthly_timeline(selected_user, df)
        fig, ax = plt.subplots()
        ax.plot(timeline['time'], timeline['message'], color='green', marker='o', label='Messages')
        ax.set_xlabel('Month')
        ax.set_ylabel('Messages')
        ax.legend(loc='upper left')
        plt.xticks(rotation='vertical')
        st.pyplot(fig)

        # Daily Timeline
        st.title("Daily Timeline")
        daily_timeline = helper.daily_timeline(selected_user, df)
        fig, ax = plt.subplots()
        ax.plot(daily_timeline['only_date'], daily_timeline['message'], color='black', label='Messages')
        ax.set_xlabel('Date')
        ax.set_ylabel('Messages')
        ax.legend(loc='upper left')
        plt.xticks(rotation='vertical')
        st.pyplot(fig)

        # Activity Map
        st.title('Activity Map')
        col1, col2 = st.columns(2)

        with col1:
            st.header("Most Busy Day")
            busy_day = helper.week_activity_map(selected_user, df)
            fig, ax = plt.subplots()
            ax.bar(busy_day.index, busy_day.values, color='pink', label='Messages')
            ax.set_xlabel('Day of Week')
            ax.set_ylabel('Messages')
            ax.legend(loc='upper left')
            plt.xticks(rotation='vertical')
            st.pyplot(fig)

        with col2:
            st.header("Most Busy Month")
            busy_month = helper.month_activity_map(selected_user, df)
            fig, ax = plt.subplots()
            ax.bar(busy_month.index, busy_month.values, color='#FFA07A', label='Messages')  # Light Orange
            ax.set_xlabel('Month')
            ax.set_ylabel('Messages')
            ax.legend(loc='upper left')
            plt.xticks(rotation='vertical')
            st.pyplot(fig)

        # Weekly Activity Map
        st.title("Weekly Activity Map")
        user_heatmap = helper.activity_heatmap(selected_user, df)
        fig, ax = plt.subplots()
        sns.heatmap(user_heatmap, ax=ax, cmap='Reds', annot=False, cbar=True)
        ax.set_xlabel('Day of Week')
        ax.set_ylabel('Week')
        st.pyplot(fig)

        # Most Busy Users (Group level)
        if selected_user == 'Overall':
            st.title('Most Busy Users')
            x, new_df = helper.most_busy_users(df)
            fig, ax = plt.subplots()
            ax.bar(x.index, x.values, color='red', label='Messages')
            ax.set_xlabel('Users')
            ax.set_ylabel('Messages')
            ax.legend(loc='upper left')
            plt.xticks(rotation='vertical')
            st.pyplot(fig)

            st.dataframe(new_df, width=800)

        # WordCloud
        st.title("Wordcloud")
        df_wc = helper.create_wordcloud(selected_user, df)
        fig, ax = plt.subplots()
        ax.imshow(df_wc, interpolation='bilinear')
        ax.axis('off')
        st.pyplot(fig)

        # Most Common Words
        st.title('Most Common Words')
        most_common_df = helper.most_common_words(selected_user, df)
        fig, ax = plt.subplots()
        ax.barh(most_common_df[0], most_common_df[1], color='purple')
        ax.set_xlabel('Frequency')
        ax.set_ylabel('Words')
        plt.xticks(rotation='horizontal')
        st.pyplot(fig)

        # Emoji Analysis
        st.title("Emoji Analysis")
        emoji_df = helper.emoji_helper(selected_user, df)
        col1, col2 = st.columns(2)

        with col1:
            st.dataframe(emoji_df, use_container_width=True)

        with col2:
            fig, ax = plt.subplots()
            ax.pie(emoji_df[1].head(), labels=emoji_df[0].head(), autopct="%0.2f%%", colors=sns.color_palette('Set2', len(emoji_df[1].head())))
            ax.set_title('Emoji Distribution')
            st.pyplot(fig)