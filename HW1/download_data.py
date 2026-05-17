from gdown import download
import re

links = ['https://drive.google.com/file/d/1_xttZm2U0fBb7rgyDQO7LJ6CDM_jFukf/view?usp=drive_link',
         'https://drive.google.com/file/d/1nOU2HjEyNnKLvGkPwTE_b0Yai2AYUNgS/view?usp=drive_link',
         'https://drive.google.com/file/d/1zL4OpfY-MemuN_GMnCM0ZxlUxSBvv4gt/view?usp=drive_link']

for link in links:
    link_id = re.search(r'd/.+/v', link).group()
    link_id = link_id[2:len(link_id)-2]
    
    clean_link = f'https://drive.google.com/uc?id={link_id}'
    
    download(clean_link)
