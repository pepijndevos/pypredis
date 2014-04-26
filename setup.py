from distutils.core import setup
from distutils.command.build import build
from distutils.command.install import install

class CFFIBuild(build):

    def finalize_options(self):
        import pypredis.reader
        self.distribution.ext_modules = [pypredis.reader.ffi.verifier.get_extension()]
        build.finalize_options(self)


class CFFIInstall(install):

    def finalize_options(self):
        import pypredis.reader
        self.distribution.ext_modules = [pypredis.reader.ffi.verifier.get_extension()]
        install.finalize_options(self)

setup(name='pypredis',
      version='0.3',
      description='A heavy-duty Redis client',
      author='Pepij de Vos',
      author_email='pepijndevos@gmail.com',
      url='https://github.com/pepijndevos/pypredis',
      install_requires=['cffi'],
      setup_requires=['cffi'],
      packages=['pypredis'],
      data_files=[('hiredis', ['hiredis/hiredis.h', 'hiredis/fmacros.h', 'hiredis/net.h', 'hiredis/sds.h', ])],
      zip_safe=False,
      cmdclass={
          "build": CFFIBuild,
          "install": CFFIInstall,
      },
     )
