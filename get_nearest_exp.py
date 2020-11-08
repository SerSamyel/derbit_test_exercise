import requests as req


def search_nearest_exp_instrument() -> str:
    """Return result-field data from url"""
    url = 'https://www.deribit.com/api/v2/public/get_instruments?currency=BTC&expired=false&kind=future'
    max_time = float('+inf')
    search_field = 'expiration_timestamp'
    curr_data = ''
    resp = req.get(url, headers={'Content-Type': 'application/json'})
    for instrument in resp.json().get('result'):
        curr_time = instrument.get(search_field)
        if curr_time < max_time:
            max_time = curr_time
            curr_data = instrument
    return curr_data.get('instrument_name')
