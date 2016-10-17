/******************************************************************************
* Copyright (c) 2016, Connor Manning (connor@hobu.co)
*
* Entwine -- Point cloud indexing
*
* Entwine is available under the terms of the LGPL2 license. See COPYING
* for specific license text and more information.
*
******************************************************************************/

#pragma once

#include <cstddef>
#include <limits>
#include <memory>
#include <numeric>
#include <vector>

#include <pdal/PointLayout.hpp>

#include <json/json.h>

#include <entwine/types/delta.hpp>
#include <entwine/types/dim-info.hpp>
#include <entwine/types/fixed-point-layout.hpp>
#include <entwine/util/json.hpp>

namespace entwine
{

class Schema
{
public:
    explicit Schema(DimList dims)
        : m_dims(dims)
        , m_layout(makePointLayout(m_dims))
    { }

    explicit Schema(const Json::Value& json)
        : Schema(
                std::accumulate(
                    json.begin(),
                    json.end(),
                    DimList(),
                    [](const DimList& in, const Json::Value& d)
                    {
                        DimList out(in);
                        out.emplace_back(d);
                        return out;
                    }))
    { }

    explicit Schema(const std::string& s) : Schema(parse(s)) { }

    template<typename T>
    Schema(std::initializer_list<T> il) : Schema(DimList(il)) { }

    Schema(const Schema& other) : Schema(other.m_dims) { }
    Schema& operator=(const Schema& other)
    {
        m_dims = other.m_dims;
        m_layout = makePointLayout(m_dims);
        return *this;
    }

    std::size_t pointSize() const
    {
        return m_layout->pointSize();
    }

    const DimList& dims() const
    {
        return m_dims;
    }

    bool contains(const std::string& name) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&name](const DimInfo& d) { return d.name() == name; }));

        return it != m_dims.end();
    }

    const DimInfo& find(const std::string& name) const
    {
        const auto it(
                std::find_if(
                    m_dims.begin(),
                    m_dims.end(),
                    [&name](const DimInfo& d) { return d.name() == name; }));

        if (it != m_dims.end()) return *it;
        else throw std::runtime_error("Dimension not found: " + name);
    }

    pdal::Dimension::Id getId(const std::string& name) const
    {
        return pdalLayout().findDim(name);
    }

    pdal::PointLayout& pdalLayout() const
    {
        return *m_layout.get();
    }

    Json::Value toJson() const
    {
        return std::accumulate(
                m_dims.begin(),
                m_dims.end(),
                Json::Value(),
                [](const Json::Value& in, const DimInfo& d)
                {
                    Json::Value out(in);
                    out.append(d.toJson());
                    return out;
                });
    }

    std::string toString() const
    {
        return std::accumulate(
                m_dims.begin(),
                m_dims.end(),
                std::string(),
                [](const std::string& s, const DimInfo& d)
                {
                    return s + (s.size() ? ", " : "") + d.name();
                });
    }

    bool normal() const
    {
        static const auto f(pdal::Dimension::BaseType::Floating);
        return
            pdal::Dimension::base(find("X").type()) == f &&
            pdal::Dimension::base(find("Y").type()) == f &&
            pdal::Dimension::base(find("Z").type()) == f;
    }

    static Schema normalize(const Schema& s)
    {
        DimList dims
        {
            pdal::Dimension::Id::X,
            pdal::Dimension::Id::Y,
            pdal::Dimension::Id::Z
        };

        for (const auto& dim : s.dims())
        {
            if (!DimInfo::isXyz(dim)) dims.emplace_back(dim);
        }

        return Schema(dims);
    };

    static Schema deltify(
            const Bounds& scaledCube,
            const Delta& delta,
            const Schema& inSchema)
    {
        pdal::Dimension::Type spatialType(pdal::Dimension::Type::Double);

        const Point ticks(
                scaledCube.width() / delta.scale().x,
                scaledCube.depth() / delta.scale().y,
                scaledCube.height() / delta.scale().z);

        auto fitsWithin([&ticks](double max)
        {
            return ticks.x < max && ticks.y < max && ticks.z < max;
        });

        if (fitsWithin(std::numeric_limits<uint32_t>::max()))
        {
            spatialType = pdal::Dimension::Type::Signed32;
        }
        else if (fitsWithin(std::numeric_limits<uint64_t>::max()))
        {
            spatialType = pdal::Dimension::Type::Signed64;
        }
        else
        {
            std::cout << "Cannot use this scale for these bounds" << std::endl;
        }

        auto isXyz([](pdal::Dimension::Id id)
        {
            return
                id == pdal::Dimension::Id::X ||
                id == pdal::Dimension::Id::Y ||
                id == pdal::Dimension::Id::Z;
        });

        DimList dims
        {
            DimInfo(pdal::Dimension::Id::X, spatialType),
            DimInfo(pdal::Dimension::Id::Y, spatialType),
            DimInfo(pdal::Dimension::Id::Z, spatialType)
        };

        for (const auto& dim : inSchema.dims())
        {
            if (!isXyz(dim.id())) dims.emplace_back(dim);
        }

        return Schema(dims);
    }

private:
    std::unique_ptr<pdal::PointLayout> makePointLayout(DimList& dims)
    {
        std::unique_ptr<pdal::PointLayout> layout(new FixedPointLayout());

        for (auto& dim : dims)
        {
            dim.setId(layout->registerOrAssignDim(dim.name(), dim.type()));
        }

        layout->finalize();

        return layout;
    }

    DimList m_dims;
    std::unique_ptr<pdal::PointLayout> m_layout;
};

inline bool operator==(const Schema& lhs, const Schema& rhs)
{
    return lhs.dims() == rhs.dims();
}

inline bool operator!=(const Schema& lhs, const Schema& rhs)
{
    return !(lhs == rhs);
}

inline std::ostream& operator<<(std::ostream& os, const Schema& schema)
{
    os << "[";

    for (std::size_t i(0); i < schema.dims().size(); ++i)
    {
        const auto& d(schema.dims()[i]);
        os << "\n\t";
        os <<
            "{ \"name\": \"" << d.name() << "\"" <<
            ", \"type\": \"" << d.typeString() << "\"" <<
            ", \"size\": " << d.size() << " }";
        if (i != schema.dims().size() - 1) os << ",";
    }

    os << "\n]";
    return os;
}

} // namespace entwine

