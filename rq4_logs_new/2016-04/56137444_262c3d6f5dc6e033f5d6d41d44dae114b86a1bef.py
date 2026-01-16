from fabric.api import local


def models():
    local('swagger-codegen generate -i specs/basic.json -l csharp -o ./tmp/')