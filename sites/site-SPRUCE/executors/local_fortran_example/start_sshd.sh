#!/bin/bash
echo ${CELERY_SSH_PASSWORD} 
echo "${CELERY_SSH_USER}:${CELERY_SSH_PASSWORD}" | chpasswd
/usr/sbin/sshd -D 

pip install -r /home/$CELERY_SSH_USER/requirements.txt