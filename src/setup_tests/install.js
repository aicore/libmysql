import {exec_shell} from '../utils/shell_utils';

export function install_mysql() {
    return exec_shell('sudo apt-get install mysql-server -y');
}

export function uninstall_mysql() {
    return exec_shell('sudo apt-get remove mysql-server -y');
}
