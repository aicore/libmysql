import child_process from 'child_process';


export async function exec_shell(cmd) {
    return new Promise((resolve, reject) => {
        child_process.exec(cmd, (error, stdout, stderr) => {
            if (error) {
                const errorMessage = `error: ${error.message}`;
                console.log(errorMessage);
                reject(errorMessage);
                return;
            }
            if (stderr) {
                const errorMessage = `stderr: ${stderr}`;
                console.log(errorMessage);
                reject(errorMessage);
                return;
            }
            const successMessage = `stdout: ${stdout}`;
            console.log(successMessage);
            resolve(successMessage);
        });

    });
}

async function demo()
{
    const msg = await exec_shell('sudo ls -lhrt');
    console.log(msg);
}
