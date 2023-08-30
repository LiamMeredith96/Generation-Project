from app import read_csv_to_list
                                
                    
<<<<<<< HEAD
# def test_read_csv_to_list():
#     dummy_data = [
#         {'id': '5', 'animal': 'cat'}, 
#         {'id': 'five', 'animal': 'dog'}
#     ]
    
#     expected = [
#         {'id': 5, 'animal': 'cat'}, 
#         {'id': None, 'animal': 'dog'}
#     ]
    
#     result = read_csv_to_list(dummy_data)
    
#     assert result == expected
=======
def test_read_csv_to_list():
    dummy_data = [
        {'id': '5', 'animal': 'cat'}, 
        {'id': 'five', 'animal': 'dog'}
    ]
    
    expected = [
        {'id': 5, 'animal': 'cat'}, 
        {'id': None, 'animal': 'dog'}
    ]
    
    result = read_csv_to_list(dummy_data)
    
    assert result == expected
>>>>>>> 06240b9e354b779fdfab4277d09f6f06acac0a85
    
    
