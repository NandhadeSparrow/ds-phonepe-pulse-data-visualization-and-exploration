import streamlit as st


def make_clickable(name, url):
    return f'<a href="{url}" target="_blank">{name}</a>'


def df_with_link(df, name_col, url_col):
    df[name_col] = df.apply(
        lambda row: make_clickable(row[name_col], row[url_col]), axis=1)
    df_mod = df.drop(url_col, axis=1)
    st.markdown(df_mod.to_html(escape=False), unsafe_allow_html=True)