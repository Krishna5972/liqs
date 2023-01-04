import smtplib
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email import encoders
from email.mime.text import MIMEText


def send_mail(filename,subject='SARAVANA BHAVA'):
    from_= 'gannamanenilakshmi1978@gmail.com'
    to= 'vamsikrishnagannamaneni@gmail.com'
    
    message = MIMEMultipart()
    message['From'] = from_
    message['To'] = to
    message['Subject'] =subject
    body_email ='SARAVANA BHAVA !'
    
    message.attach(MIMEText(body_email, 'plain'))
    
    attachment = open(filename, 'rb')
    
    x = MIMEBase('application', 'octet-stream')
    x.set_payload((attachment).read())
    encoders.encode_base64(x)
    
    x.add_header('Content-Disposition', 'attachment; filename= %s' % filename)
    message.attach(x)
    
    s_e = smtplib.SMTP('smtp.gmail.com', 587)
    s_e.starttls()
    
    s_e.login(from_, 'upsprgwjgtxdbwki')
    text = message.as_string()
    s_e.sendmail(from_, to, text)
    print(f'Sent {filename}')