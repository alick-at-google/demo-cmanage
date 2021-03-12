from unittest.mock import Mock
import main

#test for dashboards
data = 'Content Usage Last Accessed Date,Dashboard ID (User-defined only),Dashboard Title,Look ID,Look Title,Look Is Used on Dashboard (Yes / No)\n2021-02-09,987,unused_content (copy 3),,,No\n2021-02-09,988,unused_content (copy 4),,,No\n2021-02-09,989,unused_content (copy 5),,,No\n2021-02-09,972,unused_content (copy),,,No\n2021-02-09,973,unused_content (copy 2),,,No\n'

#test for looks
data = 'Content Usage Last Accessed Date,Dashboard ID (User-defined only),Dashboard Title,Look ID,Look Title,Look Is Used on Dashboard (Yes / No)\n2021-02-09,,,842,test,No\n2021-02-09,,,833,unused_looks,Yes\n2021-02-09,,,839,test soft delete 1,No\n2021-02-09,,,838,test hard delete,No\n2021-02-09,,,837,test soft delete,No\n2021-02-09,,,840,test hard delete 1,Yes\n'

#test for no results
data = 'Content Usage Last Accessed Date,Dashboard ID (User-defined only),Dashboard Title,Look ID,Look Title,Look Is Used on Dashboard (Yes / No)\n'

# test for dashboard + look input
data = 'Content Usage Last Accessed Date,Dashboard ID (User-defined only),Dashboard Title,Look ID,Look Title,Look Is Used on Dashboard (Yes / No)\n2021-02-09,,,842,test,No\n2021-02-09,,,833,unused_looks,Yes\n2021-02-09,,,839,test soft delete 1,No\n2021-02-09,,,838,test hard delete,No\n2021-02-09,,,837,test soft delete,No\n2021-02-09,,,840,test hard delete 1,Yes\n2021-02-09,988,unused_content (copy 4),,,No\n2021-02-09,989,unused_content (copy 5),,,No\n2021-02-09,972,unused_content (copy),,,No\n2021-02-09,973,unused_content (copy 2),,,No\n'

# test multiple multiple users (with multiple content to delete) - check to see if only 1 email is sent
data = 'Content Usage Last Accessed Date,Dashboard ID (User-defined only),Dashboard Title,Look ID,Look Title,Look Is Used on Dashboard (Yes / No)\n2021-02-09,,,842,test,No\n2021-02-09,989,unused_content (copy 5),,,No\n2020-12-04,918,Test B,,,No\n2020-10-29,801,(3) Gene Deep Dive,,,No\n2020-10-08,366,Test,,,No\n'


req = Mock(get_json=Mock(return_value=data), args=data)

# Call tested function
# main.main(req)
print(req)