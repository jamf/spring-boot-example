#!/bin/bash

# Install es-rollup-manager as a service starting automatically at boot, then start it.
service_name="spring-boot-example"
serviceuser="ubuntu"

if [[ $EUID -ne 0 ]]; then
   echo "This script must be run as root. Use 'sudo'." 1>&2
   exit 1
fi

install_dir=$(dirname "$(readlink -f "$0")")

if [ ! -f "$install_dir/$service_name" ]; then
   echo "File '$service_name' not found. This script should be run from inside $service_name directory." 1>&2
   exit 1
fi

#echo "Enter the name of the Linux user/account that is going to run the service:"
#read serviceuser

if id "$serviceuser" >/dev/null 2>&1; then
	echo "Installing service"
else
    echo "User does not exist"
	exit 1
fi

echo "[Unit]" > $install_dir/$service_name.service
echo "Description=$service_name deamon" >> $install_dir/$service_name.service
echo "Wants=network-online.target" >> $install_dir/$service_name.service
echo "After=network.target network-online.target elasticsearch.service" >> $install_dir/$service_name.service
echo "" >> $install_dir/$service_name.service
echo "[Service]" >> $install_dir/$service_name.service
echo "Type=forking" >> $install_dir/$service_name.service
echo "ExecStart=$install_dir/$service_name start" >> $install_dir/$service_name.service
echo "ExecStop=$install_dir/$service_name stop" >> $install_dir/$service_name.service
echo "ExecReload=$install_dir/$service_name restart" >> $install_dir/$service_name.service
echo "WorkingDirectory=$install_dir" >> $install_dir/$service_name.service
echo "User=$serviceuser" >> $install_dir/$service_name.service
echo "Group=$serviceuser" >> $install_dir/$service_name.service
echo "" >> $install_dir/$service_name.service
echo "[Install]" >> $install_dir/$service_name.service
echo "WantedBy=default.target" >> $install_dir/$service_name.service


mv -f $install_dir/${service_name}.service /lib/systemd/system/$service_name.service
systemctl daemon-reload
systemctl enable $service_name.service
systemctl start $service_name.service
