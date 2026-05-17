from gdown import download
import re

links = ['https://drive.google.com/file/d/1OXNYxcKg3cmnImKpak6rKlSbHskf8wSD/view?usp=drive_link',
         'https://drive.google.com/file/d/1eT_XhxQyYBBMJckbQpNH464b_nZU1Y60/view?usp=drive_link',
         'https://drive.google.com/file/d/1qp64-sfya38Gs2LLMTvDJJsyif2fdLSB/view?usp=drive_link']

for link in links:
    link_id = re.search(r'd/.+/v', link).group()
    link_id = link_id[2:len(link_id)-2]
    
    clean_link = f'https://drive.google.com/uc?id={link_id}'
    
    download(clean_link)
