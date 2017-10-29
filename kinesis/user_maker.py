class User:
    def __init__(self, first_name, last_name, age, gender, job, address, email):
        self.first_name = first_name
        self.last_name = last_name
        self.age = age
        self.gender = gender
        self.job = job
        self.address = address
        self.email = email

    @property
    def user_name(self):
        return self.first_name + ' ' + self.last_name

    @property
    def user_job(self):
        return self.user_name + " is a " + self.job

    @property
    def user_address(self):
        return self.user_name + " lives at " + self.address

    def __str__(self):
        sb = []
        for key in self.__dict__:
            sb.append('{key}={value}'.format(key=key,
                                             value=self.__dict__[key]))

        return ', '.join(sb)

    def __repr__(self):
        return self.__str__()

