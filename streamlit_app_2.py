# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col, when_matched
import snowflake.snowpark.session as snsession


# Write directly to the app
st.title(":cup_with_straw: Pending Smoothie Orders :cup_with_straw:")
st.write("Orders that need to filled:")

session = get_active_session()
my_dataframe = session.table("smoothies.public.orders").filter(col("ORDER_FILLED")==0).collect()

if my_dataframe:
    
    editable_df = st.experimental_data_editor(my_dataframe)
    submitted = st.button('Submit')

    if submitted:
        st.success('Someone clicked the button.👍')
    
        og_dataset = session.table("smoothies.public.orders")
        edited_dataset = session.create_dataframe(editable_df)
    
        # Remove duplicates based on the 'name_on_order' column
        edited_dataset = edited_dataset.drop_duplicates(["name_on_order"])
    
        try:
            og_dataset.merge(edited_dataset
                         , (og_dataset['order_uid'] == edited_dataset['order_uid'])
                         , [when_matched().update({'ORDER_FILLED': edited_dataset['ORDER_FILLED']})]
                        )
            st.success("Order(s) Updated 👍")
        except:
            st.write('Something went wrong')

else:
    st.success('There are no pending orders right now 👍')



