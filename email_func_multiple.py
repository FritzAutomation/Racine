import smtplib
import datetime as dt
import os
from glob import glob
from string import Template
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


# change directory to the Reports_PD folder
current_dir = os.getcwd()

# create a todays date/time variable to use in naming the file
todays_date = str(dt.datetime.now().strftime("%Y-%m-%d_%H_%M")) + ".xlsx"


def get_contacts(filename):
    """
    Return two lists names, emails containing names and email addresses
    read from a file specified by filename.
    """

    names = []
    emails = []
    with open(filename, mode="r", encoding="utf-8") as contacts_file:
        for a_contact in contacts_file:
            names.append(a_contact.split()[0])
            emails.append(a_contact.split()[1])
    return names, emails


def read_template(filename):
    """
    Returns a Template object comprising the contents of the
    file specified by filename.
    """

    with open(filename, "r", encoding="utf-8") as template_file:
        template_file_content = template_file.read()
    return Template(template_file_content)


def main(
    contacts_filename,
    message_filename,
    message_subject,
    file_title,
    file_dir,
    second_file,
):
    names, emails = get_contacts(contacts_filename)  # read contacts
    message_template = read_template(message_filename)

    # For each contact, send the email:
    for name, email in zip(names, emails):
        # add in the actual person name to the message template
        # msg = message_template.attach(MIMEText(message_template.safe_substitute({'PERSON_NAME': name}), "html"))

        # print(email)

        subject = message_subject
        sender_email = "System.Reporting@Cnhind.com"
        receiver_email = email

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = receiver_email
        message["Subject"] = subject
        message["Bcc"] = receiver_email  # Recommended for mass emails

        # Add body to email
        message.attach(
            MIMEText(
                message_template.safe_substitute({"PERSON_NAME": name.title()}), "html"
            )
        )

        # Directory and file title information
        current_dir = r"C:\FritzAutomation\Racine"
        file_dir = r"\reports\\"
        file_title = "RAC_Unit_EOL_Report_"  # Adjust this to your specific prefix

        # Get the latest file in the specified directory that matches the file_title prefix
        files = glob(os.path.join(current_dir + file_dir, f"{file_title}*"))

        if files:
            # Find the latest file by modification time
            latest_file = max(files, key=os.path.getmtime)
            print(f"Latest file found: {latest_file}")
        else:
            raise FileNotFoundError("No files found with the specified prefix.")

        # Update filelist to use the latest file dynamically
        filelist = [latest_file, second_file]

        for file in filelist:
            # Open report file in binary mode
            with open(file, "rb") as attachment:
                # Add file as application/octet-stream
                # Email client can usually download this automatically as attachment
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())

                # Encode file in ASCII characters to send by email
                encoders.encode_base64(part)

                # Add header as key/value pair to attachment part
                part.add_header(
                    "Content-Disposition",
                    "attachment; filename= %s" % os.path.basename(file),
                )

                message.attach(part)

        # Add attachment to message and convert message to string
        text = message.as_string()

        # # Log in to server using secure context and send email
        # with smtplib.SMTP(host="mailrac.casecorp.com", port=25) as server:
        #     server.sendmail(sender_email, receiver_email, text)
        #     server.quit()

        smtp_servers = [
            {"host": "mailrac.casecorp.com", "port": 25},
            # {"host": "SSK1SAP1.cnh1.cnhgroup.cnh.com", "port": 25},
        ]

        for server_info in smtp_servers:
            try:
                with smtplib.SMTP(
                    host=server_info["host"], port=server_info["port"]
                ) as server:
                    server.sendmail(sender_email, receiver_email, text)
                    print(
                        f"Email sent to {name.title()} successfully using {server_info['host']}"
                    )
                    # return  # Exit the function after a successful send
            except Exception as e:
                print(f"Failed to send email using {server_info['host']}: {e}")
                pass


if __name__ == "__main__":
    main()
