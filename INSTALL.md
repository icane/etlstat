.gitignore# Instalación de requisitos

## pygit2
Para instalar la versión actualizada de la librería libgit2, debemos ejecutar estos pasos en consola:
```
 wget https://github.com/libgit2/libgit2/archive/v0.26.0.tar.gz
 tar xzf v0.26.0.tar.gz
 cd libgit2-0.26.0/
 cmake .
 make
 sudo make install
```
y sus dependencias:
```
 apt install build-essential libffi-dev python-dev libgit2-dev
```
Posteriormente instalamos el paquete pygit2:
```
pip3 install pygit2
```
Puede ocurrir el siguiente error al intentar importar el paquete en nuestro proyecto Python:
```
ImportError: libgit2.so.0: cannot open shared object file: No such file or directory
```
Esto es debido a que la librería libgit2 se instala en /usr/local/lib, pero el linker
no está configurado para buscar en ese directorio. Se soluciona ejecutando el siguiente comando:
```
sudo ldconfig
```