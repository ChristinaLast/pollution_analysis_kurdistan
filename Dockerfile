RUN curl -sSL https://install.python-poetry.org | python3 -
RUN brew install pyenv
RUN echo -e 'export PYENV_ROOT="$HOME/.pyenv" \
    export PATH="$PYENV_ROOT/bin:$PATH" \
    eval "$(pyenv init --path)" \
    eval "$(pyenv init -)"' >> ~/.bash_profile
RUN source ~/.bash_profile

RUN pyenv global 3.8.0
RUN export PATH="$HOME/.poetry/bin:$PATH"
WORKDIR 
RUN 