from gdown import download
import re

links = ['https://drive.google.com/file/d/1y7QduDqBR7iAYO6r16MiHz0xXm63IBe_/view?usp=drive_link',
         'https://drive.google.com/file/d/15Ri5KPs7k2X5O-dEU50jWus0SmiSbqRb/view?usp=drive_link',
         'https://drive.google.com/file/d/11DB62UGmKMH_bNni2fRUVW_34ExqYTcA/view?usp=drive_link',
         'https://drive.google.com/file/d/19P8MT9RDJaimIXA71JcNZ5BPzwU948h4/view?usp=drive_link']

for link in links:
    link_id = re.search(r'd/.+/v', link).group()
    link_id = link_id[2:len(link_id)-2]
    
    clean_link = f'https://drive.google.com/uc?id={link_id}'
    
    download(clean_link)
