segments = {
    'frequency_segment': {
        '0-4': {'lower': 0, 'upper': 4},
        '5-13': {'lower': 5, 'upper': 13},
        '14-37': {'lower': 14, 'upper': 37},
        '38+': {'lower': 38, 'upper': ''},
    },
    'recency_segment': {
        '0-30': {'lower': 0, 'upper': 30},
        '31-60': {'lower': 31, 'upper': 60},
        '61-90': {'lower': 61, 'upper': 90},
        '91-120': {'lower': 91, 'upper': 120},
        '121-180': {'lower': 121, 'upper': 180},
        '181+': {'lower': 181, 'upper': ''},
    }
}

# num = 5
# for key, value in segments['frequency_segment'].items():
#     if value['lower'] <= num <= value['upper']:
#         print(key)
