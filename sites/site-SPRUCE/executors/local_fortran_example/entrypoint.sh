#!/bin/sh
chown -R $CELERY_SSH_USER:$CELERY_SSH_USER /data
# Jian: give the privileges of webData
chown -R $CELERY_SSH_USER:$CELERY_SSH_USER /webData
/root/start_sshd.sh