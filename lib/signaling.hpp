//
// Created by evan on 5/29/19.
//

#ifndef SEALM_SIGNALING_HPP
#define SEALM_SIGNALING_HPP

#include <vector>
#include <memory>

class Observer {
public:
    virtual void update(int event) = 0;
};

class Observable {
private:
    std::vector<std::shared_ptr<Observer> > _observers;
public:
    void register_observer(std::shared_ptr<Observer> o) { _observers.emplace_back(o); }

    void notify(int event){
        for (auto const &o : _observers){
            o->update(event);
        }
    }
};

#endif //SEALM_SIGNALING_HPP
