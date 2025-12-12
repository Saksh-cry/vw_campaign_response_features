import streamlit as st
import pandas as pd
import joblib
MODEL_PATH = "vw_campaign_response_features.pkl"
model = joblib.load(MODEL_PATH)
st.title("vw_campaign_response_features")




event1=st.number_input("customer_id", min_value=0, value=0)
event2=st.number_input("email", min_value=0, value=0)
event3=st.number_input("past_clicks", min_value=0, value=0)
event4=st.number_input("past_order_value", min_value=0, value=0)
event5=st.number_input("total_tickets", min_value=0, value=0)
event6=st.number_input("clicked_flag", min_value=0, value=0)


if st.button("Predict"):
    input_df = pd.DataFrame([{
       'customer_id':event1, 
       'email':event2,
       'past_clicks':event3, 
       'past_order_value':event4, 
       'total_tickets':event5 ,
       'clicked_flag':event6,
      
       }])
    
    pred= model.predict(input_df)[0]
    st.success(f"Prediction: {'Not Approved' if pred == 1 else'Approved'}")


